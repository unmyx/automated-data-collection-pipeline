# Architecture

This document is the visual companion to [`PLAN.md`](PLAN.md). The plan is the
source of truth for *why*; this file shows *what the code looks like* and *what
happens, in order, during a run*. Every diagram is Mermaid, so GitHub renders it
without any build step.

**Related:** [README](../README.md) · [PORTFOLIO](PORTFOLIO.md) ·
[DEMO](DEMO.md) · [RUNBOOK](RUNBOOK.md) · [queries.sql](queries.sql)

---

## 1. System context

```mermaid
flowchart LR
    subgraph operator["Operator"]
        cron["cron / systemd timer / CI"]
        human["developer at a shell"]
    end

    subgraph adcp["ADCP service (Python 3.12+)"]
        cli["adcp CLI<br/>collect · schedule · db · config"]
        scheduler["scheduler.py<br/>APScheduler wrapper"]
        service["pipeline/service.py<br/>CollectionService"]
        api["api/open_meteo.py<br/>httpx adapter"]
        validation["validation/<br/>4 layers"]
        db["db/<br/>repositories + lock"]
    end

    subgraph external["External systems"]
        openmeteo["Open-Meteo REST API<br/>public, JSON"]
        postgres[("PostgreSQL 17")]
    end

    cron -->|"adcp collect"| cli
    human -->|"adcp collect"| cli
    human -->|"adcp schedule"| cli
    cli --> service
    cli --> scheduler
    scheduler -->|"same job body as CLI"| service
    service --> api
    service --> validation
    service --> db
    api -->|"hourly forecast/archive"| openmeteo
    db -->|"SQLAlchemy 2.0 Core + psycopg 3"| postgres
```

The CLI never talks to the API or the database directly: it composes settings,
the adapter, and the service, then maps the returned `RunSummary` to an exit
code. That keeps one collection path for manual runs, scheduled runs, and tests.

---

## 2. Layering (ports and adapters)

```mermaid
flowchart TB
    subgraph inbound["Inbound adapters"]
        cliLayer["cli/"]
        schedLayer["scheduler.py"]
    end

    subgraph core["Application core (no I/O frameworks)"]
        svc["pipeline/service.py"]
        win["pipeline/window.py"]
        val["validation/"]
        norm["normalization.py<br/>row_hash"]
        models["models/<br/>Location · Observation · Run"]
        ports["ports.py<br/>WeatherSource protocol"]
    end

    subgraph outbound["Outbound adapters"]
        apiImpl["api/open_meteo.py"]
        repos["db/repository.py<br/>db/run_tracker.py<br/>db/watermark_store.py"]
        lockImpl["db/lock.py"]
        eng["db/engine.py"]
    end

    cliLayer --> svc
    schedLayer --> svc
    svc --> win
    svc --> val
    svc --> norm
    svc --> models
    svc -.->|"depends on the protocol"| ports
    ports -.->|"implemented by"| apiImpl
    svc --> repos
    svc --> lockImpl
    repos --> eng
    lockImpl --> eng
    eng --> pg[("PostgreSQL")]
```

Two rules are enforced by `tests/test_architecture.py` rather than by convention:

1. `adcp.api` must not import `adcp.db` - the client can be exercised without a
   database, and a database outage cannot change request construction.
2. HTTP is confined to `adcp.api` and `adcp.resilience` - no other module opens a
   connection.

---

## 3. Data flow: one `adcp collect` run

```mermaid
flowchart TD
    start(["adcp collect"]) --> cfg["load + validate Settings<br/>exit 2 on bad config"]
    cfg --> lock{"pg_try_advisory_lock<br/>collection"}
    lock -- "not acquired" --> skipped["log ingest.run.skipped<br/>exit 0 (nothing to do)"]
    lock -- acquired --> reap["reap stale runs<br/>(running > 2 x run_timeout_s)"]
    reap --> load["load active locations<br/>+ per-source watermarks"]
    load --> empty{"any locations?"}
    empty -- no --> nothing["log ingest.run.skipped<br/>exit 0"]
    empty -- yes --> runrow["INSERT ingestion_runs (running)"]

    runrow --> perloc["for each location<br/>(bounded concurrency)"]

    subgraph location["one location - one transaction"]
        plan["plan window<br/>storage window + accept range"]
        fetch["GET Open-Meteo<br/>timeouts + retries"]
        schema["schema parse<br/>strict pydantic models"]
        domain["domain validation<br/>ranges, alignment, source"]
        referential["referential checks<br/>location identity, coordinates"]
        transform["normalise + row_hash"]
        upsert["INSERT ... ON CONFLICT<br/>DO UPDATE WHERE row_hash IS DISTINCT"]
        water["advance watermark"]
        plan --> fetch --> schema --> domain --> referential --> transform --> upsert --> water
    end

    perloc --> location
    location -- commit --> counters["update run counters"]
    location -- "exception / reject budget" --> isolated["rollback THIS location only<br/>record ingestion_run_errors"]
    isolated --> counters
    counters --> more{"more locations?"}
    more -- yes --> perloc
    more -- no --> status["compute status:<br/>succeeded / partial / failed"]
    status --> finish["finish run row<br/>+ emit ingest.run.completed"]
    finish --> unlock["release advisory lock"]
    unlock --> code{"exit code"}
    code -- succeeded --> c0["0"]
    code -- partial --> c3["3"]
    code -- failed --> c1["1"]
```

The three properties worth calling out:

- **Per-location transactions.** `_persist()` opens one transaction per location.
  A failure rolls back that location's rows *and* its watermark; every other
  location that already committed stays committed.
- **Watermark after commit only.** `watermark_store.advance()` runs inside the
  same transaction as the upsert, so the cursor can never point past data that
  was rolled back.
- **Lock first, everything else second.** A second process (or an overlapping
  scheduler tick) fails fast with `ingest.run.skipped` instead of double-writing.

---

## 4. Sequence: one location

```mermaid
sequenceDiagram
    autonumber
    participant S as CollectionService
    participant W as WatermarkStore
    participant A as OpenMeteoClient
    participant O as Open-Meteo
    participant V as validation/
    participant R as WeatherRepository

    S->>W: get(location_id, source)
    W-->>S: last_observed_at (or None)
    Note over S: plan storage window =<br/>[watermark - overlap, current hour)<br/>capped by lookback
    S->>A: fetch_hourly(location, source, window)
    A->>O: GET /v1/forecast?...
    alt transient failure (timeout / 5xx / 429)
        O--xA: error
        A->>A: full-jitter backoff, bounded
        A->>O: retry
    end
    O-->>A: JSON payload
    A->>A: strict schema parse -> WeatherSeries
    A-->>S: WeatherSeries
    S->>V: validate + place rows in the window
    V-->>S: accepted rows + reasoned rejections
    Note over S: rows outside the storage window are<br/>SKIPPED, not rejected (forecast tail)
    S->>R: upsert_in_transaction(rows)
    R-->>S: inserted / updated / unchanged
    S->>W: advance(...)   %% same transaction
    Note over S,R: COMMIT
```

---

## 5. Database schema

Five tables; `weather_hourly` is the fact table and the other four are the
configuration, audit, and cursor tables around it. The DDL in
[`PLAN.md`](PLAN.md#5-postgresql-schema) section 5 is the source of truth, and
`src/adcp/db/tables.py` mirrors it so `alembic check` can detect drift.

```mermaid
erDiagram
    locations ||--o{ weather_hourly : "location_id"
    locations ||--o{ ingestion_watermarks : "location_id"
    locations ||--o{ ingestion_run_errors : "location_id (nullable)"
    ingestion_runs ||--o{ weather_hourly : "first_seen_run_id"
    ingestion_runs ||--o{ weather_hourly : "last_seen_run_id"
    ingestion_runs ||--o{ ingestion_run_errors : "run_id"
    ingestion_runs |o--o{ ingestion_watermarks : "last_run_id"

    locations {
        bigint id PK
        text slug UK "kebab-case, checked"
        text name
        numeric latitude "between -90 and 90"
        numeric longitude "between -180 and 180"
        text timezone "default UTC"
        char country_code "nullable"
        boolean is_active
        timestamptz created_at
        timestamptz updated_at
    }

    weather_hourly {
        bigint id PK
        bigint location_id FK
        timestamptz observed_at "hour-aligned, checked"
        text source "forecast | historical_forecast | archive"
        numeric temperature_2m "13 measurement columns, nullable"
        text row_hash "content hash of the normalised row"
        numeric upstream_latitude
        numeric upstream_longitude
        numeric upstream_elevation_m
        text upstream_timezone
        uuid first_seen_run_id FK
        uuid last_seen_run_id FK
        timestamptz first_collected_at
        timestamptz last_collected_at
        int revision_count
    }

    ingestion_runs {
        uuid id PK
        text run_type
        text trigger "cli | scheduler | cron | ci"
        ingestion_status status "running|succeeded|partial|failed|skipped"
        timestamptz window_from
        timestamptz window_to
        int locations_total
        int locations_succeeded
        int locations_failed
        int requests_made
        int requests_retried
        int rows_received
        int rows_inserted
        int rows_updated
        int rows_unchanged
        int rows_rejected
        int error_count
        text error_summary
        text app_version
        text hostname
        timestamptz started_at
        timestamptz finished_at
        int duration_ms
    }

    ingestion_run_errors {
        bigint id PK
        uuid run_id FK
        bigint location_id FK "nullable"
        text phase "fetch | validate | persist | run"
        text error_type
        text error_code
        text message
        smallint attempt
        smallint http_status
        text request_url
        jsonb payload_sample
        timestamptz occurred_at
    }

    ingestion_watermarks {
        bigint location_id PK
        text source PK
        timestamptz last_observed_at
        uuid last_run_id FK "nullable"
        timestamptz updated_at
    }
```

### The natural key and the no-op upsert

`weather_hourly_natural_key` is `(location_id, observed_at, source)`. Because
`source` is part of the key, a **forecast** and an **archive** observation of the
same hour coexist instead of overwriting each other. The write is:

```sql
INSERT INTO weather_hourly (...) VALUES (...)
ON CONFLICT (location_id, observed_at, source) DO UPDATE
   SET ...
 WHERE weather_hourly.row_hash IS DISTINCT FROM EXCLUDED.row_hash;
```

The `WHERE` clause is what makes a re-run a *true storage no-op*: identical data
produces no update, so `last_collected_at` and `revision_count` are untouched -
not merely rewritten with the same values.

---

## 6. The incremental window

Two different windows are involved, and conflating them is how a pipeline either
loses data or rejects half of every payload:

```mermaid
flowchart LR
    subgraph timeline["UTC timeline (current hour = 12:00, lookback 3h, overlap 2h)"]
        direction LR
        past["accept range start<br/>= start of day - past_days"]
        wm["watermark<br/>11:00"]
        start["storage start<br/>max(11:00 - 2h, 12:00 - 3h) = 09:00"]
        end["storage end<br/>12:00 (exclusive)"]
        future["accept range end<br/>= start of day + 1 day"]
    end
```

| Region | Classification | Why |
| --- | --- | --- |
| `[storage_start, storage_end)` | **stored** | completed hours this run may write |
| inside the accept range, outside the storage window | **skipped** | the forecast tail and the already-covered hours; the next run will observe them properly |
| outside the accept range | **rejected** (`OutOfWindow`) | the provider returned something we never asked for - a bug, not noise |

The storage window's start is `watermark - overlap`, capped by
`now - lookback`. That is why the overlap setting is the lever for re-reading
recent hours after a model revision, while `lookback` caps how far back a first
run (or a gap recovery) may reach.

---

## 7. Validation layers

Rows move through four layers; the first failure that applies decides whether the
row is rejected, and every rejection carries a machine-readable code.

```mermaid
flowchart TD
    payload["HTTP payload"] --> transport{"transport<br/>status, size, JSON?"}
    transport -- bad --> e1["SchemaError / UpstreamError<br/>never retried if permanent"]
    transport -- ok --> schema{"schema<br/>pydantic strict models"}
    schema -- bad --> e2["SchemaError with field paths"]
    schema -- ok --> domain{"domain<br/>ranges, hour alignment, source"}
    domain -- bad --> e3["Rejection: OutOfRange /<br/>OutOfDomain / SourceMismatch ..."]
    domain -- ok --> referential{"referential<br/>location id, coordinates, window"}
    referential -- bad --> e4["Rejection: LocationMismatch /<br/>CoordinateDrift / OutOfWindow"]
    referential -- ok --> accepted["accepted for upsert"]
```

Transport and schema failures fail the whole fetch (the payload cannot be
trusted). Domain and referential failures reject individual rows but let the rest
of the location commit - subject to the per-location reject budget
(`ADCP_INGEST_MAX_INVALID_ROW_RATIO`, default 25%). Above the budget the location
rolls back entirely, which is what stops a bad provider response from half-filling
the table.

---

## 8. Failure and retry behaviour

### Client-level classification

```mermaid
flowchart TD
    fail["request failed"] --> kind{"exception type"}
    kind -- "ConnectError / ConnectTimeout<br/>ReadTimeout / WriteTimeout" --> retry["RetryableUpstreamError"]
    kind -- "429 Too Many Requests" --> retryAfter["retryable + honour Retry-After<br/>(capped by backoff max)"]
    kind -- "500 / 502 / 503 / 504" --> retry
    kind -- "400 / 401 / 403 / 404 / 422" --> permanent["permanent - fail the location"]
    kind -- "malformed JSON / oversized body" --> permanent
    kind -- "schema / validation error" --> permanent
    retry --> policy{"attempts left AND<br/>request budget left?"}
    retryAfter --> policy
    policy -- yes --> sleep["sleep: full-jitter exponential,<br/>clamped to the budget"]
    sleep --> attempt["retry the request"]
    policy -- no --> giveup["raise to the caller"]
    permanent --> giveup
    attempt -.-> fail
```

Only `RetryableUpstreamError` is retried. Malformed requests, invalid
configuration, schema/semantic validation errors, and 4xx responses are never
retried, because waiting cannot make them true.

### Run-level behaviour

```mermaid
stateDiagram-v2
    [*] --> running : lock acquired, run row created
    running --> succeeded : every location committed, no rejections
    running --> partial : some locations failed OR some rows rejected
    running --> failed : failure budget exceeded / run budget exceeded
    running --> skipped : lock not acquired, or no active locations
    succeeded --> [*]
    partial --> [*]
    failed --> [*]
    skipped --> [*]
```

| Situation | Behaviour | Exit code |
| --- | --- | --- |
| Everything commits, nothing rejected | `succeeded` | 0 |
| Lock held by another process | no run row, `ingest.run.skipped` | 0 |
| No active locations | no run row, `ingest.run.skipped` | 0 |
| Some locations fail, others commit | `partial`, failed locations get error rows | 3 |
| Rejections inside the per-location budget | `partial` | 3 |
| More than `ADCP_INGEST_FAILURE_BUDGET_RATIO` of locations fail | `failed` | 1 |
| Run wall-clock budget exhausted | `failed`, `RunTimeoutExceeded`; committed locations survive | 1 |
| Database unreachable | clean one-line error | 1 |
| Invalid flag value or unknown location | clean one-line error | 2 |
| `Ctrl+C` | interrupts, cleans up | 130 |

Full catalogue, including the F1-F17 failure identifiers: [`PLAN.md`](PLAN.md#11-failure-and-partial-failure-behaviour)
section 11.

---

## 9. Operational workflow

```mermaid
flowchart LR
    subgraph prep["Prepare"]
        p1["adcp config check"]
        p2["adcp db upgrade"]
        p3["seed locations"]
    end
    subgraph steady["Steady state"]
        s1["adcp schedule<br/>(hourly at :07 UTC)"]
        s2["or cron: adcp collect"]
    end
    subgraph observe["Observe"]
        o1["ingestion_runs<br/>status + counters"]
        o2["ingestion_watermarks<br/>freshness"]
        o3["ingestion_run_errors<br/>failure digest"]
        o4["structured JSON logs"]
    end
    subgraph recover["Recover"]
        r1["adcp collect<br/>idempotent retry"]
        r2["adcp collect --lookback-hours N<br/>widen a gap"]
    end
    prep --> steady --> observe
    observe -- "failed / partial / stale" --> recover --> steady
```

The recovery step is deliberately boring: because writes are idempotent and the
watermark only moves on commit, "run it again" is always safe, and widening the
lookback re-reads more history without duplicating anything. The step-by-step
operator procedure is in [`RUNBOOK.md`](RUNBOOK.md).

---

## 10. Where each concern lives

| Concern | Module |
| --- | --- |
| Configuration and validation | `src/adcp/config.py` |
| CLI assembly and exit codes | `src/adcp/cli/`, `src/adcp/exit_codes.py` |
| HTTP client and retry policy | `src/adcp/api/open_meteo.py`, `src/adcp/resilience.py` |
| Wire models and request construction | `src/adcp/api/schemas.py`, `src/adcp/api/requests.py`, `src/adcp/api/mapping.py` |
| Domain models | `src/adcp/models/` |
| Validation layers | `src/adcp/validation/` |
| Canonical values and `row_hash` | `src/adcp/normalization.py` |
| Run orchestration | `src/adcp/pipeline/service.py` |
| Window planning | `src/adcp/pipeline/window.py` |
| Schema and upserts | `src/adcp/db/tables.py`, `src/adcp/db/repository.py` |
| Run tracking | `src/adcp/db/run_tracker.py` |
| Watermarks | `src/adcp/db/watermark_store.py` |
| Advisory lock | `src/adcp/db/lock.py` |
| Migrations | `src/adcp/db/migrations/` |
| Scheduling | `src/adcp/scheduler.py` |
| Structured logging and redaction | `src/adcp/logging.py` |
