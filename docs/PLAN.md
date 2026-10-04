# ADCP - Automated Data Collection Pipeline

**Design and delivery plan**

| | |
| --- | --- |
| Status | M1-M7 complete - all milestones implemented and verified |
| Owner | Daniel |
| Last updated | 2026-10-04 |
| Source of truth | This document. Milestone exit criteria are normative. |
| Implementation | Every behaviour described as implemented is present in `src/adcp` and covered by the test suite. Sections 13, 16, and 17 mark what was deliberately not built. |

---

## Table of contents

1. [Business/problem statement](#1-businessproblem-statement)
2. [Scope and non-goals](#2-scope-and-non-goals)
3. [Architecture](#3-architecture)
4. [Data flow](#4-data-flow)
5. [PostgreSQL schema](#5-postgresql-schema)
6. [API integration design](#6-api-integration-design)
7. [Scheduling approach](#7-scheduling-approach)
8. [Retry/timeout strategy](#8-retrytimeout-strategy)
9. [Validation rules](#9-validation-rules)
10. [Idempotency strategy](#10-idempotency-strategy)
11. [Failure and partial-failure behaviour](#11-failure-and-partial-failure-behaviour)
12. [Logging and observability](#12-logging-and-observability)
13. [CLI design](#13-cli-design)
14. [Testing strategy](#14-testing-strategy)
15. [Configuration/environment variables](#15-configurationenvironment-variables)
16. [Repository structure](#16-repository-structure)
17. [Milestones M1 onward](#17-milestones-m1-onward)
18. [Portfolio/demo requirements](#18-portfoliodemo-requirements)

---

## 1. Business/problem statement

### 1.1 The problem

Teams that consume weather data need a **reliable, replayable, and queryable hourly
history** for a fixed set of locations. Naive collection scripts fail in predictable
ways:

- they run late, get killed, or overlap with themselves and write duplicate rows;
- they treat upstream data as immutable even though forecast models are **revised
  several times a day**, so a "collected once" row silently becomes stale;
- they fail silently - nobody notices that a location has had no data for two days;
- they are impossible to audit because nothing records *what* ran, *when*, and
  *which locations failed*.

The result is a dataset that looks complete but has gaps and duplications, and no
operational trail to prove otherwise.

### 1.2 The solution

A production-style Python service that:

1. runs on a schedule (hourly, with catch-up after downtime);
2. pulls hourly weather variables for configured locations from the public
   Open-Meteo REST API;
3. validates the payload before it touches the database;
4. writes to PostgreSQL **idempotently**, keyed on
   `(location, observed_at, source)`, so any window can be re-fetched safely;
5. records every run, per-location attempt, and error for audit and alerting;
6. emits structured JSON logs so failures are searchable;
7. behaves predictably under partial failure - one bad location never loses the
   other nineteen;
8. exposes a CLI that is equally usable by a human debugging locally and by cron in
   production.

### 1.3 Portfolio goal

This repository is the artefact. It is designed to be *read* by a hiring manager or
senior engineer and to demonstrate, in working code, the following competences:

| Competence | Demonstrated by |
| --- | --- |
| Python API integration | `httpx` client with typed request/response models |
| REST/JSON ingestion | Open-Meteo forecast + archive endpoints, documented field mapping |
| PostgreSQL | Normalised schema, constraints, indexes, upserts, Alembic migrations |
| Scheduled collection | One-shot command + scheduler wrapper, DB-backed single-run guarantee |
| Incremental ingestion | Watermarks + revision overlap window |
| Idempotent writes | Natural-key upsert with content hashing; rerun yields zero new rows |
| Validation | Layered transport/schema/domain validation with quarantine |
| Retries and timeouts | Explicit per-phase timeouts, exponential backoff with jitter, retry taxonomy |
| Structured logging | `structlog` JSON pipeline with bound run/location context |
| Ingestion/run tracking | `ingestion_runs`, `ingestion_run_errors`, `ingestion_watermarks` |
| Automated tests | Unit + integration + contract tests, DB-backed idempotency tests |
| Clean project architecture | Ports/adapters layering, typed config, no business logic in the CLI |

### 1.4 Success measures

- A clean checkout reaches a working `adcp collect` in under five minutes.
- Running the same collection twice in a row inserts zero duplicate rows and reports
  `0 inserted / 0 updated` on the second pass.
- Killing the process mid-run leaves the database consistent and the run row marked
  `failed`; re-running completes the work.
- Every failure mode in section 8 and section 11 has an automated test.
- `pytest --cov` reports >= 85% line and branch coverage on `src/adcp`.

---

## 2. Scope and non-goals

### 2.1 In scope

- A configurable set of locations (latitude/longitude pairs with a stable slug).
- Hourly variables: 2 m temperature, relative humidity, 2 m dew point, apparent
  temperature, precipitation, rain, snowfall, weather code, cloud cover, mean sea
  level pressure, 10 m wind speed/direction, 10 m wind gusts.
- Two upstream sources: **forecast/recent** (`past_days` window) and **historical
  archive** (ERA5, used for backfill).
- Scheduled incremental collection with a configurable lookback and revision overlap.
- Backfill over an explicit date range, chunked into month-sized requests.
- Idempotent persistence with run tracking, error tracking, and watermarks.
- Structured logging, run summaries, and operational health queries.
- Dockerised local development (PostgreSQL + optional Adminer), a production
  `Dockerfile`, and a Compose `app` profile.
- Unit, integration, and contract tests, plus CI on GitHub Actions.

### 2.2 Non-goals

| Non-goal | Rationale |
| --- | --- |
| A web UI or dashboard | Out of scope for a data-ingestion portfolio piece. SQL recipes + Adminer cover demo needs. |
| Forecast modelling, ML, or analytics | The pipeline *collects and stores* data; it does not interpret it. |
| Streaming / sub-hourly data | Open-Meteo's free hourly endpoints are the target; Kafka-style streaming adds no portfolio value here. |
| Multi-tenant SaaS features | A single logical dataset, single database. Locations are configuration, not tenants. |
| Commercial redistribution of Open-Meteo data | The free tier is non-commercial; the project documents attribution and licence terms. |
| Orchestrators (Airflow, Dagster, Prefect) | The scheduler is deliberately small. Section 7 explains when an orchestrator *would* be justified. |
| An HTTP API for served data | Query PostgreSQL directly; a read API would be a separate service. |
| Secrets management (Vault, cloud KMS) | Environment variables with documented production guidance are sufficient here. |
| Exactly-once delivery semantics | Deliberately chose **at-least-once delivery + idempotent writes**, which is the honest and practical guarantee. |

---

## 3. Architecture

### 3.1 Style

Ports and adapters (hexagonal), with a synchronous core and async I/O at the edges.
The dependency rule points inward: the pipeline domain knows nothing about `httpx`,
`psycopg`, or `typer`.

```
                         +-------------------------------+
   process boundary      |  cli/  (typer commands)       |
                         |    collect | backfill | runs |
                         |    schedule | config | db    |
                         +---------------+---------------+
                                         | calls
                         +---------------v---------------+
                         |  pipeline/  (orchestration)   |
                         |  IngestionService             |
                         |   - plan run window           |
                         |   - per-location isolation    |
                         |   - run accounting            |
                         +----+--------+--------+--------+
                              |        |        |
             ports (Protocol) |        |        |
                +-------------v-+  +---v------+ +v--------------+
                | WeatherSource |  | Weather  | | RunTracker /  |
                | (protocol)    |  | Repository | WatermarkStore |
                +-------+-------+  +----+-----+ +-------+-------+
                        |               |               |
          adapters      |               |               |
                +-------v-------+ +-----v--------+ +----v----------+
                | api/open_meteo| | db/repository| | db/run_tracker|
                | httpx+tenacity| | SQLAlchemy   | | SQLAlchemy    |
                +-------+-------+ +-----+--------+ +----+----------+
                        |               |               |
              infrastructure: httpx    psycopg3 / SQLAlchemy 2.0
                                          |
                                    +-----v------+
                                    | PostgreSQL |
                                    +------------+

   cross-cutting: config/ (pydantic-settings), logging/ (structlog), errors/, exit_codes
```

### 3.2 Components

| Layer | Module (planned) | Responsibility | Must not |
| --- | --- | --- | --- |
| Presentation | `adcp.cli` | Parse arguments, load config, call the service, map results to exit codes, render output | Contain ingestion logic or SQL |
| Application | `adcp.pipeline` | Orchestrate a run: plan window, iterate locations, isolate failures, account for totals | Know about HTTP or SQL details |
| Domain | `adcp.models`, `adcp.validation` | Typed observation records, source enum, validation rules, content hashing | Import `httpx`, `sqlalchemy`, or `typer` |
| Ports | `adcp.ports` | `WeatherSource`, `WeatherRepository`, `RunTracker`, `WatermarkStore` protocols | Reference concrete adapters |
| Adapters | `adcp.api`, `adcp.db` | HTTP transport + response mapping; SQL persistence, upserts, run/error records | Leak transport models into the domain |
| Infra | `adcp.config`, `adcp.logging`, `adcp.resilience`, `adcp.errors`, `adcp.exit_codes` | Settings, structured logging, retry policy, exception hierarchy, process exit codes | Hold business rules |

### 3.3 Technology choices and trade-offs

| Decision | Choice | Alternatives considered | Why |
| --- | --- | --- | --- |
| HTTP client | `httpx` (synchronous) | `requests`, `aiohttp` | Typed, fine-grained per-phase timeouts, `respx` for tests, and the same client can go async later without a rewrite (ADR-006) |
| Validation | `pydantic` v2 | `marshmallow`, hand-rolled | Declarative, fast (Rust core), excellent error messages, shared with settings |
| Configuration | `pydantic-settings` | `dynaconf`, `python-dotenv` + dataclasses | Typed, `.env` aware, validated at startup, no bespoke parsing |
| Database access | SQLAlchemy 2.0 Core + lightweight repository (`psycopg` 3, synchronous) | Raw `psycopg`, Django ORM, ORM entities, async SQLAlchemy | Core tables keep DDL, migrations, and upserts in one vocabulary; `psycopg3` serves both sync and async, so the door stays open; unit-of-work via explicit transactions |
| Migrations | Alembic | `sqlalchemy.create_all`, `sqitch` | Versioned, reversible, reviewable DDL in code review |
| Logging | `structlog` | stdlib `logging` + `json-log-formatter` | Key-value binding, processor pipeline, JSON/console switch without code changes |
| Retries | `tenacity` | `urllib3.Retry`, hand-rolled loop | Declarative policies, jitter, `wait` strategies, easy to unit test the policy |
| CLI | `typer` | `click`, `argparse` | Type-hint driven, subcommands, built-in test runner, rich help |
| Scheduling | APScheduler inside a `schedule` command, plus support for external cron | Airflow, Celery beat | Section 7 - a single-process job does not need a cluster |
| Tests | `pytest` + `respx` + `testcontainers` | `unittest` + `responses` | Ecosystem standard; real PostgreSQL for upsert and lock semantics; no `pytest-asyncio` needed because the code is synchronous (ADR-006) |

### 3.4 Key architectural decisions

- **ADR-001: At-least-once + idempotent writes.** Duplicate delivery is cheap
  insurance against silent data loss. The unique key `(location_id, observed_at,
  source)` makes replays harmless.
- **ADR-002: The one-shot command is the contract.** `adcp collect` must be safe to
  run at any time, by any scheduler. The built-in scheduler is a convenience wrapper,
  never the only way to run the pipeline.
- **ADR-003: Per-location transactions.** Isolation keeps one failing location from
  rolling back the work of nineteen others, and made the partial-failure semantics in
  section 11 testable.
- **ADR-004: Store what upstream said, plus provenance.** Separate `source` values
  and a `collected_at`/`run_id` trail mean a revised forecast creates a new version
  rather than destroying history (see section 5.4 for the history option).
- **ADR-005: No hidden global state.** Settings, logger, and clients are constructed
  at the composition root (`cli/`) and injected. This is what makes the test suite
  fast and deterministic.
- **ADR-006: Synchronous I/O with a bounded thread pool (supersedes the M1 draft).**
  The M1 draft proposed asyncio for the fetch layer. M2 revisits that decision now
  that the database stack is real; section 3.5 documents the choice and why.

### 3.5 Concurrency model

**Decision (M2): synchronous end to end, with a bounded thread pool for per-location
concurrency.** The M1 draft proposed asyncio; M2 deliberately reverses that before
any pipeline code exists, because the database layer settles the question.

```python
with ThreadPoolExecutor(max_workers=settings.open_meteo_max_concurrency) as pool:
    for location, result in pool.map(collect_one_location, locations):
        ...
```

Why synchronous wins here:

| Criterion | Synchronous + thread pool | Asyncio + async everywhere |
| --- | --- | --- |
| Fits the work | One blocking HTTP GET and one short write per location; a backoff in one worker never blocks another | Same throughput for 5-20 locations/hour; async buys nothing at this scale |
| Fits the database | Core + `psycopg` sync is the documented happy path; one transaction per location is plain code | Needs an async engine, async sessions, and `run_sync` for Alembic - more moving parts for the same behaviour |
| Fits the scheduler | APScheduler's `BlockingScheduler` is synchronous; one concurrency model for the whole process | Mixing `asyncio.run` inside a blocking scheduler invites event-loop leaks and signal-handling bugs |
| Testability | Plain `def test_...`, no `pytest-asyncio`, no event-loop fixtures; failures produce ordinary tracebacks | Needs `pytest-asyncio`/`anyio` configuration around every async test |
| Explainability | "Four workers, each doing fetch -> validate -> write sequentially" | Requires explaining the event loop, task cancellation, and blocking-call pitfalls |
| Migration path | `httpx` and `psycopg` both have async APIs, so a future move touches adapters, not the domain | - |

Concrete rules that follow from the decision:

- the pipeline core is **sync**; `ThreadPoolExecutor` with
  `max_workers=ADCP_OPEN_METEO_MAX_CONCURRENCY` (default 4) bounds concurrency so a
  20-location run does not open 20 sockets at once;
- **database writes stay sequential** - each worker owns its connection for the
  duration of one location's transaction, and connections come from the pool sized by
  `ADCP_DB_POOL_MIN_SIZE`/`ADCP_DB_POOL_MAX_SIZE` (section 15.3);
- retries use `tenacity`'s synchronous support; jitter replaces coroutine scheduling;
- the CLI and the scheduler (M5) remain synchronous, with `KeyboardInterrupt` and
  SIGTERM handled by the process rather than by task cancellation.

Revisit if: collection moves to thousands of locations, a sub-minute cadence is
required, or the API gains streaming endpoints. None of those are in scope.

---

## 4. Data flow

### 4.1 Scheduled run, end to end

```
[scheduler tick / manual CLI / cron]
        |
        | 1. acquire advisory lock  pg_try_advisory_lock('adcp:collect')
        v
[run begins] ------------------------------------------------------+
        |                                                          |
        | 2. load + validate settings (fail fast, exit code 2)      |
        | 3. load active locations                                  |
        | 4. insert ingestion_runs row (status=running)             |
        |                                                          |
        | 5. for each location (bounded concurrency):               |
        |      a. read watermark -> compute [from, to] window       |
        |         from = watermark - overlap_hours                  |
        |         to   = now floored to the hour                    |
        |      b. GET Open-Meteo (timeout + retry + jitter)         |
        |      c. parse JSON -> typed payload (schema validation)   |
        |      d. validate rows (domain rules) -> accepted/rejected  |
        |      e. BEGIN; upsert accepted rows; update watermark;     |
        |         COMMIT  (one transaction per location)            |
        |      f. record per-location attempt counters/errors        |
        |                                                          |
        | 6. finalise run row: status, counters, duration           |
        | 7. release advisory lock                                  |
        v                                                          |
[exit code: 0 ok | 3 partial | 1 failed | 2 config error] <--------+
```

### 4.2 Step detail

| Step | Input | Output | Failure behaviour |
| --- | --- | --- | --- |
| 1 Lock | lock name | `True`/`False` | Lock held -> log `ingest.run.skipped`, exit 0 (nothing to do) |
| 2 Config | env, `.env`, flags | immutable `Settings` | Any validation error -> exit 2, nothing written |
| 3 Locations | `locations` table | enabled rows | Empty set -> log warning, exit 0 |
| 4 Run row | settings, trigger | `run_id` (UUID) | DB down -> exit 1, no partial state |
| 5a Window | watermark, settings, clock | `[from, to]` UTC | Watermark missing -> full default lookback |
| 5b Fetch | window, location | HTTP response | Retries exhausted -> location marked failed, continue |
| 5c Parse | bytes | `OpenMeteoResponse` | Invalid JSON/shape -> location failed with `SchemaError` |
| 5d Validate | response rows | accepted rows + rejects | Reject ratio over budget -> location failed (no write) |
| 5e Write | accepted rows | insert/update counters | DB error -> rollback that location only |
| 6 Finalise | per-location results | run row + exit code | Always attempted, even after catastrophic failure |

### 4.3 Incremental ingestion and the overlap window

Watermarks advance monotonically, but the *fetch window* deliberately reaches
backwards:

```
   watermark              from = watermark - overlap        to = now(hour)
       |                            |                            |
       v                            v                            v
  -----+----------------------------+----------------------------+----> time
       |<------ re-fetched -------->|<------ new data ---------->|
       ^
       rows already stored; upsert finds them unchanged (hash match, no write)
```

Why: Open-Meteo's forecast for "today 14:00" is revised as the model runs again. Re-reading the
last `ADCP_INGEST_OVERLAP_HOURS` (default 24) hours lets the pipeline correct itself.
The **content hash** means unchanged rows are not rewritten (section 10), and the
watermark only moves forward once a location's transaction commits.

Why not "only fetch what is missing": that is faster but wrong for forecast data.
The overlap is the difference between a dataset that converges on the truth and one
that freezes the first guess forever.

### 4.4 Backfill flow (designed, not implemented)

`adcp backfill --from 2024-01-01 --to 2024-03-31`:

1. resolves locations the same way as a scheduled run;
2. splits the range into month-sized chunks (Open-Meteo handles multi-year ranges,
   but chunking bounds the blast radius of a failure and keeps payloads small);
3. routes each chunk to the correct source: the **archive** endpoint for ranges older
   than the archive delay, the **historical forecast** endpoint for the recent past;
4. processes chunks oldest-first and **does not** consult watermarks (an explicit
   range means "make this range true");
5. records one `ingestion_runs` row with `trigger='backfill'`, plus chunk-level
   counters, so a backfill is as auditable as a scheduled run.

The supporting pieces exist (`DateRange` in `models/window.py`, the archive and
historical-forecast endpoints in `config.py`, `source` in the natural key, and
the `adcp_open_meteo_chunk_pause_s` setting), but the command itself and its
source routing were not built. This is the largest remaining gap; see section 17.

---

## 5. PostgreSQL schema

Target: PostgreSQL 17. All timestamps are `timestamptz` stored in UTC. DDL lives in
Alembic migrations from M2; the definitions below are the contract those migrations
must satisfy.

### 5.1 Entity relationships

```
 locations 1 --- n ingestion_watermarks          (per source)
 locations 1 --- n weather_hourly                (per source)
 locations 1 --- n ingestion_run_errors
 locations 1 --- n backfill_jobs ? (M5, optional)
 ingestion_runs 1 --- n ingestion_run_errors
 ingestion_runs 1 --- n weather_hourly           (last_seen_run_id / first_seen_run_id)
```

### 5.2 `locations` - configuration-as-data

```sql
CREATE TABLE locations (
    id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    slug            text        NOT NULL,                 -- stable, url-safe identifier
    name            text        NOT NULL,
    latitude        numeric(9,6)  NOT NULL,               -- -90 .. 90
    longitude       numeric(9,6)  NOT NULL,               -- -180 .. 180
    timezone        text        NOT NULL DEFAULT 'UTC',   -- IANA name, for reporting only
    country_code    char(2),
    is_active       boolean     NOT NULL DEFAULT true,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT locations_slug_key        UNIQUE (slug),
    CONSTRAINT locations_lat_range       CHECK (latitude  BETWEEN -90  AND 90),
    CONSTRAINT locations_lon_range       CHECK (longitude BETWEEN -180 AND 180),
    CONSTRAINT locations_slug_format     CHECK (slug ~ '^[a-z0-9]+(-[a-z0-9]+)*$')
);

CREATE INDEX locations_active_idx ON locations (is_active) WHERE is_active;
```

`is_active = false` retires a location without deleting its history - the pipeline
skips it, historical queries still see it.

### 5.3 `weather_hourly` - the fact table

```sql
CREATE TABLE weather_hourly (
    id                      bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    location_id             bigint      NOT NULL REFERENCES locations (id) ON DELETE CASCADE,
    observed_at             timestamptz NOT NULL,   -- hour, always UTC, minute=0, second=0
    source                  text        NOT NULL,   -- 'forecast' | 'historical_forecast' | 'archive'

    -- measurements (nullable: sparse upstream fields are legitimate)
    temperature_2m          numeric(5,2),           -- degC
    relative_humidity_2m    numeric(5,2),           -- %
    dew_point_2m            numeric(5,2),           -- degC
    apparent_temperature    numeric(5,2),           -- degC
    precipitation           numeric(6,2),           -- mm
    rain                    numeric(6,2),           -- mm
    snowfall                numeric(6,2),           -- cm
    weather_code            smallint,               -- WMO code
    cloud_cover             numeric(5,2),           -- %
    pressure_msl            numeric(7,2),           -- hPa
    wind_speed_10m          numeric(6,2),           -- km/h
    wind_direction_10m      smallint,               -- deg
    wind_gusts_10m          numeric(6,2),           -- km/h

    -- provenance
    row_hash                text        NOT NULL,   -- sha256 of normalised measurements
    upstream_latitude       numeric(9,6) NOT NULL,  -- grid cell the provider resolved
    upstream_longitude      numeric(9,6) NOT NULL,
    upstream_elevation_m    numeric(7,2),
    upstream_timezone       text,
    first_seen_run_id       uuid        NOT NULL REFERENCES ingestion_runs (id),
    last_seen_run_id        uuid        NOT NULL REFERENCES ingestion_runs (id),
    first_collected_at      timestamptz NOT NULL DEFAULT now(),
    last_collected_at       timestamptz NOT NULL DEFAULT now(),
    revision_count          integer     NOT NULL DEFAULT 0,

    CONSTRAINT weather_hourly_natural_key  UNIQUE (location_id, observed_at, source),
    CONSTRAINT weather_hourly_hour_aligned CHECK (date_trunc('hour', observed_at) = observed_at),
    CONSTRAINT weather_hourly_source_valid CHECK (source IN ('forecast','historical_forecast','archive'))
);

CREATE INDEX weather_hourly_location_time_idx ON weather_hourly (location_id, observed_at DESC);
CREATE INDEX weather_hourly_observed_at_idx   ON weather_hourly (observed_at DESC);
CREATE INDEX weather_hourly_source_idx        ON weather_hourly (source, observed_at DESC);
```

Design notes:

- **Natural key, not surrogate key, is the idempotency anchor.** The surrogate `id`
  exists for convenience; `UNIQUE (location_id, observed_at, source)` is what makes
  the upsert safe under concurrency.
- **`source` is part of the key.** A forecast row and an archive row for the same
  hour are *different measurements of reality* and must both exist - that difference
  is what makes forecast-accuracy analysis possible later.
- **`numeric`, not `float`.** Reproducible equality, clean hashing, and no
  0.1 + 0.2 surprises in the hash function.
- **Sparse fields stay nullable.** Open-Meteo legitimately returns `null` for some
  variables in some models; a null is information, not an error.
- **`revision_count`** increments when an upsert actually changes values, turning
  "how often does the forecast revise?" into a one-line query.

### 5.4 Optional history table (deferred, not implemented)

The fact table keeps the *latest* view of an hour. If full revision history is
required, add an append-only `weather_hourly_revisions` table written by the same
transaction, or convert `weather_hourly` to a range-partitioned table on
`observed_at` with `(location_id, observed_at, source, collected_at)` as the key.
Deferred because it multiplies storage with no portfolio-critical benefit; the
design keeps the door open because provenance columns already exist.

### 5.5 `ingestion_runs` - the audit anchor

```sql
CREATE TYPE ingestion_status AS ENUM ('running','succeeded','partial','failed','skipped');

CREATE TABLE ingestion_runs (
    id                  uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    run_type            text NOT NULL,              -- 'scheduled' | 'manual' | 'backfill'
    trigger             text NOT NULL,              -- 'cli' | 'scheduler' | 'cron' | 'ci'
    status              ingestion_status NOT NULL DEFAULT 'running',
    requested_from      timestamptz,                -- backfill range, else NULL
    requested_to        timestamptz,
    window_from         timestamptz,                -- effective per-run floor
    window_to           timestamptz,
    locations_total     integer NOT NULL DEFAULT 0,
    locations_succeeded integer NOT NULL DEFAULT 0,
    locations_failed    integer NOT NULL DEFAULT 0,
    requests_made       integer NOT NULL DEFAULT 0,
    requests_retried    integer NOT NULL DEFAULT 0,
    rows_received       integer NOT NULL DEFAULT 0,
    rows_inserted       integer NOT NULL DEFAULT 0,
    rows_updated        integer NOT NULL DEFAULT 0,
    rows_unchanged      integer NOT NULL DEFAULT 0,
    rows_rejected       integer NOT NULL DEFAULT 0,
    error_count         integer NOT NULL DEFAULT 0,
    error_summary       text,                       -- short human-readable digest
    app_version         text NOT NULL,
    hostname            text,
    started_at          timestamptz NOT NULL DEFAULT now(),
    finished_at         timestamptz,
    duration_ms         integer,

    CONSTRAINT ingestion_runs_finished_after_started
        CHECK (finished_at IS NULL OR finished_at >= started_at)
);

CREATE INDEX ingestion_runs_started_at_idx ON ingestion_runs (started_at DESC);
CREATE INDEX ingestion_runs_status_idx     ON ingestion_runs (status) WHERE status <> 'succeeded';
CREATE INDEX ingestion_runs_open_idx       ON ingestion_runs (started_at) WHERE finished_at IS NULL;
```

`ingestion_runs` is written in three phases: `INSERT` when the run starts (status
`running`), periodic counter updates while locations complete, and a final `UPDATE`
that sets the terminal status and duration. The `runs` CLI command reads it.

### 5.6 `ingestion_run_errors` - why it failed, not just that it failed

```sql
CREATE TABLE ingestion_run_errors (
    id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id         uuid NOT NULL REFERENCES ingestion_runs (id) ON DELETE CASCADE,
    location_id    bigint REFERENCES locations (id) ON DELETE SET NULL,
    phase          text NOT NULL,      -- 'config'|'fetch'|'parse'|'validate'|'write'|'finalise'
    error_type     text NOT NULL,      -- exception class name, stable for alerting
    error_code     text,               -- upstream HTTP status / provider reason code
    message        text NOT NULL,
    attempt        smallint,           -- retry attempt number, 1-based
    http_status    smallint,
    request_url    text,               -- query string only; never credentials
    payload_sample jsonb,              -- truncated raw payload (max 4 KB) for forensics
    occurred_at    timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX ingestion_run_errors_run_idx      ON ingestion_run_errors (run_id);
CREATE INDEX ingestion_run_errors_error_type_idx ON ingestion_run_errors (error_type, occurred_at DESC);
```

**Redaction rule:** `request_url` is passed through the credential masker, and
`payload_sample` is bounded before insert. The M1 sketch suggested
`left(payload::text, 4096)::jsonb`, but truncating serialised JSON and casting it
back produces *invalid* JSON - so M4 stores an oversized sample as
`{"truncated": true, "excerpt": "..."}` instead, which stays valid `jsonb` and
under the 4 KB budget.

### 5.7 `ingestion_watermarks` - the incremental cursor

```sql
CREATE TABLE ingestion_watermarks (
    location_id      bigint NOT NULL REFERENCES locations (id) ON DELETE CASCADE,
    source           text   NOT NULL,
    last_observed_at timestamptz NOT NULL,   -- max(observed_at) successfully committed
    last_run_id      uuid REFERENCES ingestion_runs (id) ON DELETE SET NULL,
    updated_at       timestamptz NOT NULL DEFAULT now(),

    PRIMARY KEY (location_id, source)
);
```

One row per location per source. It is updated **inside the same transaction** as the
row upserts: if the transaction rolls back, the watermark does not move, and the next
run re-requests the same window. This is the mechanism that makes crash recovery
correct rather than merely hopeful.

### 5.8 Retention and maintenance

| Table | Growth | Policy |
| --- | --- | --- |
| `weather_hourly` | 24 rows/day/location/source -> ~8.8k rows/year/location | Keep indefinitely; partition by year at ~50M rows |
| `ingestion_runs` | 24 rows/day at hourly cadence | Keep 12 months, then archive |
| `ingestion_run_errors` | Bursts on incidents | Keep 90 days, then delete |
| `locations`, `ingestion_watermarks` | Trivial | Keep |

Implemented in M6 as `adcp db prune --runs-older-than-days 365 --errors-older-than-days 90`
with a **dry-run default** (`--apply` deletes). One honest caveat the implementation
surfaced: `weather_hourly.first_seen_run_id`/`last_seen_run_id` reference the runs
that produced each row, so deleting a run that observations point at would destroy
provenance. Those runs are kept and counted separately
(`runs_kept_by_provenance`), which is why the plan calls the policy "archive"
rather than "delete". Error rows age out independently of their run.

### 5.9 Migration strategy

- Every schema change is an Alembic revision; `schema_version` is checked at startup
  and a mismatch aborts the run with exit code 2.
- Migrations are **additive first** (add column -> backfill -> enforce), so a rolling
  deploy never reads a column that does not exist yet.
- CI runs `alembic upgrade head` and a `downgrade -1`/`upgrade head` round trip.
- `alembic check` runs in CI to catch models that drifted from migrations.

---

## 6. API integration design

### 6.1 Upstream endpoints

| Purpose | Endpoint | Notes |
| --- | --- | --- |
| Recent + forecast hours | `GET https://api.open-meteo.com/v1/forecast` | `past_days=N&forecast_days=1`, hourly variables |
| Past forecasts (2022+, no delay) | `GET https://historical-forecast-api.open-meteo.com/v1/forecast` | Same schema; used to fill the recent past cheaply |
| Archive / reanalysis (1940+, ~5-day delay) | `GET https://archive-api.open-meteo.com/v1/archive` | ERA5; used for backfill beyond the delay window |

All three share the same response envelope, so a single response model and a single
mapper serve all three; the endpoint only changes the `source` value recorded.

### 6.2 Request construction

```
GET {base_url}
    ?latitude={lat}
    &longitude={lon}
    &hourly=temperature_2m,relative_humidity_2m,dew_point_2m,apparent_temperature,
            precipitation,rain,snowfall,weather_code,cloud_cover,pressure_msl,
            wind_speed_10m,wind_direction_10m,wind_gusts_10m
    &timezone=UTC                     # deterministic, tz-aware-on-read semantics
    &start_date=YYYY-MM-DD            # explicit windows (backfill)
    &end_date=YYYY-MM-DD
    &cell_selection=nearest
    # or, for scheduled runs:
    &past_days={lookback_days}&forecast_days=1
```

Rules:

- **Always pin `timezone=UTC`.** With a local timezone the response contains naive
  local timestamps plus a `utc_offset_seconds` hint, which is easy to misread and
  hard to store correctly. Pinning UTC keeps every timestamp unambiguous.
- **Prefer explicit `start_date`/`end_date` for backfill**, `past_days`/`forecast_days`
  for scheduled runs. Open-Meteo rejects mutually exclusive combinations.
- **Request only the variables the schema stores.** Fewer bytes, less validation
  surface, faster runs.
- **Location batching is a later optimisation.** Open-Meteo accepts comma-separated
  coordinate lists and returns an array. It reduces request count but couples the
  failure of N locations into one response - a direct conflict with per-location
  isolation (§11). Plan: keep one request per location; revisit only if rate limits
  become binding, and then batch in small groups (<= 5) with per-element validation.

### 6.3 HTTP policy

| Concern | Decision |
| --- | --- |
| Client lifetime | One `httpx.Client` per run (synchronous, ADR-006), owned by `OpenMeteoClient` and closed by its context manager; `base_url` unset so explicit URLs keep logs honest |
| `User-Agent` | `ADCP_OPEN_METEO_USER_AGENT`, includes version + repo URL |
| Default headers | `Accept: application/json`, `Accept-Encoding: gzip` |
| Timeouts | One `httpx.Timeout` per client, built from settings, covering connect/read/write/pool (see §8.1) - never the "no timeout" default |
| Whole-request budget | `ADCP_OPEN_METEO_TIMEOUT_TOTAL_S` bounds attempts *and* sleeps for one logical request; the retry loop clamps its last backoff to whatever is left |
| HTTP/2 | Off. Not needed; keeps dependencies and failure modes smaller |
| Redirects | Follow up to 3 |
| Response size cap | Reject bodies > 5 MB (guards against pathological payloads), enforced while streaming and from `Content-Length` when present |
| Caching | Honour `Cache-Control` awareness only in logging; the pipeline's own watermark is the cache |
| Concurrency | A bounded `ThreadPoolExecutor(ADCP_OPEN_METEO_MAX_CONCURRENCY)` in the pipeline (M4); the client's connection pool is capped at the same number so a run cannot open more sockets than it intends |
| Auth | None for the free tier. `ADCP_OPEN_METEO_API_KEY` is reserved for a commercial key and injected as a query parameter if set |
| Retry-After | Honoured on 429/503, capped at `ADCP_OPEN_METEO_BACKOFF_MAX_S` |
| Response parsing | Strict pydantic wire models (`api/schemas.py`) plus a translation step (`api/mapping.py`) that raises structured `SchemaError`s - no raw dictionaries leave the adapter |

Log events emitted by the adapter: `api.request.started`, `api.request.retry`,
`api.request.completed`, `api.request.failed`, `api.response.coordinate_drift`.

### 6.4 Response model and field mapping

```jsonc
{
  "latitude": 44.8125, "longitude": 20.4375, "generationtime_ms": 0.42,
  "utc_offset_seconds": 0, "timezone": "GMT", "timezone_abbreviation": "GMT",
  "elevation": 138.0,
  "hourly_units": { "time": "iso8601", "temperature_2m": "°C", "...": "..." },
  "hourly": {
    "time": ["2026-09-30T00:00", "2026-09-30T01:00", "..."],
    "temperature_2m": [17.4, 16.9, "..."],
    "relative_humidity_2m": [63, 66, "..."]
  }
}
```

Mapping rules:

1. `latitude`/`longitude` in the response are the provider's **grid cell centre**,
   not the requested point. They are stored in `upstream_*` columns and compared
   against the request with a tolerance (see §9.4); a mismatch is a warning, not a
   rejection.
2. `elevation` (metres) -> `upstream_elevation_m`.
3. `timezone`/`timezone_abbreviation` -> `upstream_timezone` (provenance only).
4. `hourly_units` is checked against expected units (`°C`, `%`, `mm`, `cm`, `hPa`,
   `km/h`, `W/m²`, `iso8601`). A unit change is a **hard validation failure** - it
   means the schema's assumptions are wrong.
5. `hourly.time[i]` is parsed and localised using the *requested* timezone (UTC),
   then combined with each variable at index `i`.
6. Arrays are zipped strictly: every variable array must have the same length as
   `time`. Ragged arrays are a schema error.
7. `null` values pass through as `NULL`; they are not coerced to zero.

### 6.5 Rate limits, licensing, and etiquette

- The free tier is rate-limited (documented guidance is on the order of thousands of
  calls/day and double-digit calls/second; **exact limits are re-verified in M3** and
  encoded as settings, not hard-coded).
- Budget: one request per location per hour. Twenty locations = 480 requests/day -
  comfortably inside the free tier, with headroom for backfills.
- The designed (not built) backfill would **throttle by concurrency cap** and add a
  small inter-chunk delay (`ADCP_OPEN_METEO_CHUNK_PAUSE_S`, validated but not
  consumed yet) so a large backfill could not monopolise the shared service.
- The client sends a descriptive `User-Agent` and never retries non-idempotent
  requests.
- Attribution: README and any exported dataset must credit Open-Meteo (CC BY 4.0)
  and respect the non-commercial free-tier terms.

---

## 7. Scheduling approach

### 7.1 Options considered

| Option | Pros | Cons | Verdict |
| --- | --- | --- | --- |
| In-process APScheduler | Zero extra infrastructure; `adcp` is self-contained | Duplicate runs if scaled; dies with the process | **Chosen for the demo default** (`adcp schedule`) - implemented in M5 |
| External cron / systemd timer | Battle-tested, no extra Python deps, survives restarts | Requires host access; less portable in a container | **Chosen for production guidance**, documented in the README |
| Docker Compose `app` service | Reproducible locally; one `up` brings the whole stack online | Container is not restarted-on-crash by default (needs `restart: unless-stopped`) | **Chosen for the Compose profile** - the `app` service runs `adcp schedule` |
| Kubernetes CronJob | Right answer at scale; built-in concurrency policy | Needs a cluster - disproportionate here | Documented, not implemented |
| Airflow / Dagster / Prefect | DAGs, retries, backfills, lineage UI | Heavy for 1 job; a scheduler for schedulers | Explicit non-goal (§2.2) |

### 7.2 Decision: one-shot command, thin scheduler wrapper

`adcp collect` is the unit of work. It is idempotent, so running it twice, or late,
or by two different schedulers in different environments, is safe. The scheduler only
decides *when*; it never owns correctness.

```bash
# cron (hourly at :07 - see 7.3)
7 * * * * cd /srv/adcp && /srv/adcp/.venv/bin/adcp collect >> /var/log/adcp/cron.log 2>&1

# systemd timer
OnCalendar=hourly
Persistent=true            # catch up after downtime

# in-process
adcp schedule --minute 7 --timezone UTC
```

### 7.3 Cadence

- Default: **hourly at HH:07** (`ADCP_SCHEDULER_MINUTE=7`). The offset avoids the
  top-of-hour stampede on the public API and gives upstream models a few minutes to
  publish.
- APScheduler configuration: `trigger="cron", minute=7, id="collect-hourly"`,
  `coalesce=True` (a missed run collapses to one), `max_instances=1`, and
  `misfire_grace_time=900` (15 minutes) so a brief pause does not lose the tick.
- `ADCP_SCHEDULER_SKIP_IF_RUNNING=true` makes an overrunning run skip, not stack.
- **Development cadence:** `ADCP_SCHEDULER_INTERVAL_SECONDS` (or
  `--interval-seconds`) replaces the hourly tick with "every N seconds" so a demo
  or a test does not wait an hour between cycles. It is configuration, not a test
  hack: the same `build_schedule` path validates both models, and values below 60
  are documented as development-only.
- `adcp schedule --run-once` collects immediately and then keeps the schedule,
  which is how a freshly deployed container warms up without waiting for the hour.

### 7.4 Single-run guarantee under concurrency

The in-process scheduler protects itself with `max_instances=1`, but that only covers
one process. Two containers, a manual `adcp collect` during a scheduled run, or a
cron overlap all break it. The real guarantee is in PostgreSQL:

```sql
-- Two-key form: a fixed ADCP namespace plus a hashed logical name, so the lock cannot
-- collide with single-key advisory locks taken by other applications in the same
-- database. Namespace 1094862672 is 0x41444350, which spells "ADCP".
SELECT pg_try_advisory_lock(1094862672, hashtext('adcp:collect'));
```

Session-level advisory lock, held for the lifetime of the run, released on process
exit (PostgreSQL drops session locks when the connection closes, including crashes).
`adcp.db.lock.advisory_lock` also releases it explicitly before the connection goes
back to the pool, and invalidates the connection if that release ever fails.
If the lock is not acquired, the run logs `ingest.run.skipped` with reason
`lock_not_acquired` and exits 0 - this is expected contention, not an error.

Implemented in M5 as three independent guards, in order of authority:

1. the advisory lock above (authoritative - it covers a second scheduler process,
   a manual `adcp collect`, and a stale run);
2. the scheduler's single-worker executor and `max_instances=1`;
3. an in-process flag that turns an overlapping tick into
   `scheduler.collection.skipped` rather than a queued run.

Alternative considered: `concurrency policy` in Kubernetes, a lock file, or a Redis
lock. The advisory lock wins because it is atomic, released on crash, and requires no
new infrastructure.

### 7.5 Time and clock concerns

- All internal time arithmetic is timezone-aware UTC (`datetime.now(UTC)`); naive
  datetimes are rejected by lint rule `DTZ`.
- `to` is always **floored to the hour** and **exclusive**: a run at 14:07 requests up
  to 14:00, because 15:00 does not exist yet.
- A clock skew of a few seconds is harmless. Any skew larger than the schedule
  interval is a host problem, and the run summary (`duration_ms`, `window_from/to`)
  makes it detectable.
- DST does not apply: UTC has no transitions, which is a deliberate simplification
  (per-location local-time reporting is a presentation concern).

### 7.6 Missed runs, gaps, and catch-up

Because the window is derived from the watermark rather than from "the last hour",
recovery is automatic:

1. the process is down for 6 hours;
2. the next run computes `from = watermark - overlap`, `to = now(hour)`;
3. the request covers the entire gap (as long as it fits the upstream `past_days`
   limit, default lookback 72h);
4. if the gap is larger than the API allows, the run logs
   `ingest.run.window_truncated` and the operator is told to run `adcp backfill
   --from <date> --to <date>` (designed, not built - see section 17; today the
   operator widens the lookback with `adcp collect --lookback-hours N`).

The designed `adcp db gaps --location <slug> --from --to` command is **not
built**; query 7 in `docs/queries.sql` finds missing hours today.

### 7.7 Operating the scheduler

`adcp schedule` is the only long-running process in the project. It validates
configuration, checks that the schema is at head (exit 2 if not, exit 1 if the
database is unreachable), prints the resolved schedule, and then blocks until it
is asked to stop.

| Concern | Behaviour |
| --- | --- |
| Startup failure | exit 1 (unreachable database) or exit 2 (scheduling disabled, bad timezone/minute/interval, schema behind head) |
| Graceful shutdown | SIGINT/SIGTERM (and SIGBREAK on Windows) call `shutdown(wait=True)`: the in-flight collection finishes, APScheduler stops, resources are released, and signal handlers are restored |
| Failure isolation | a failed collection - whether the service returned a `failed` run or the runner raised - is logged and the schedule continues; nothing about one bad cycle can stop the scheduler |
| Log events | `scheduler.started`, `scheduler.next_run`, `scheduler.collection.triggered`, `scheduler.collection.completed`, `scheduler.collection.skipped`, `scheduler.collection.failed`, `scheduler.job.missed`, `scheduler.job.error`, `scheduler.shutdown.requested`, `scheduler.stopped` |
| Operator demo | `python scripts/scheduler_demo.py --cycles 2 --interval-seconds 15` runs the real command, waits for cycles, stops it with Ctrl+Break, and verifies that no duplicate hours were written |

One implementation note that matters operationally: APScheduler dispatches job
events on the executor thread, so the listeners must never call back into the
scheduler (a job lookup there takes the job-store lock the main loop may hold,
and `shutdown(wait=True)` would then deadlock). The scheduler therefore computes
the next fire time from the trigger, which is pure arithmetic; a regression test
in `tests/test_scheduler.py` pins that behaviour.

---

## 8. Retry/timeout strategy

### 8.1 Timeouts (no unbounded waits, ever)

| Phase | Setting | Default | Rationale |
| --- | --- | --- | --- |
| TCP connect | `ADCP_OPEN_METEO_TIMEOUT_CONNECT_S` | 5 s | DNS/TCP failure should surface fast |
| TLS handshake | covered by connect | - | `httpx` folds it into connect |
| Read | `ADCP_OPEN_METEO_TIMEOUT_READ_S` | 20 s | Full-year archive payloads are the worst case |
| Write | `ADCP_OPEN_METEO_TIMEOUT_WRITE_S` | 10 s | GET has no body, but be explicit |
| Pool acquire | httpx default | 5 s | Prevents silent queueing behind a hung socket |
| Total per attempt | derived | ~35 s | Backstop for pathological half-open connections |
| Whole request (all attempts) | `ADCP_OPEN_METEO_TIMEOUT_TOTAL_S` | 60 s | The retry loop never sleeps past this budget, so one location cannot stall the run |
| DB connect | `ADCP_DB_CONNECT_TIMEOUT_S` | 5 s | Fail fast when the database is unreachable |
| DB statement | `ADCP_DB_STATEMENT_TIMEOUT_MS` | 30 000 ms | A stuck query must not hold the advisory lock forever |
| Whole run | `ADCP_RUN_TIMEOUT_S` (M4) | 3 600 s | Prevents a zombie run blocking the next hour |

An `httpx.Timeout` object is constructed once from settings and shared; per-attempt
overrides are not permitted, so timeouts are auditable in one place.

### 8.2 Failure taxonomy and retry policy

| Class | Examples | Retry? | Action |
| --- | --- | --- | --- |
| Transient network | `ConnectError`, `ConnectTimeout`, `ReadTimeout`, `RemoteProtocolError`, pooled socket closed | **Yes** | Exponential backoff + jitter |
| Rate limited | HTTP 429 | **Yes** | Backoff, honour `Retry-After` (capped at max backoff) |
| Server error | HTTP 500/502/503/504 | **Yes** | Exponential backoff + jitter |
| Client error | HTTP 400/401/403/404/422 | **No** | Permanent - record error, fail the location, alert (a 400 means our request is wrong) |
| Payload size | body > cap | **No** | Record error, fail the location |
| Malformed JSON | `json.JSONDecodeError` | **No** | Not a transient condition; fail the location |
| Schema violation | missing keys, ragged arrays, changed units | **No** | Fail the location with `SchemaError` |
| Domain validation | out-of-range values, out-of-window rows | **No** | Reject rows, continue, count rejects |
| Database | deadlock, serialization failure, connection reset | **Yes** (2 attempts) | Rollback the location transaction and retry from the top |
| Database | constraint violation, permission denied | **No** | Fail the location loudly; this is a bug or a config error |

### 8.3 Backoff shape

```
tenacity.retry(
    retry=retry_if_exception(_is_retryable),
    wait=wait_random_exponential(multiplier=ADCP_OPEN_METEO_BACKOFF_INITIAL_S,
                                 max=ADCP_OPEN_METEO_BACKOFF_MAX_S),   # full jitter
    stop=stop_after_attempt(ADCP_OPEN_METEO_MAX_ATTEMPTS),             # 5
    reraise=True,
    before_sleep=_log_retry,                                           # structured log per attempt
)
```

- Base 1 s, doubled per attempt, capped at 30 s, with **full jitter** so a fleet of
  locations does not retry in lockstep.
- Worst case per location: 5 attempts, ~60 s of waiting -> well inside the run budget.
- Every retry is logged at `WARNING` with `attempt`, `max_attempts`, `delay_s`, and
  `error_type`, and counted into `ingestion_runs.requests_retried`.
- Retries are only applied to **idempotent GET requests**; the pipeline performs no
  other kind of upstream call.
- `Retry-After` (seconds or HTTP-date) takes precedence over computed backoff, capped
  at `ADCP_OPEN_METEO_BACKOFF_MAX_S`.

### 8.4 Non-retryable upstream errors are still informative

When a location exhausts retries or hits a permanent error, the run does not die:

```
attempt 1..5 -> FAILED -> ingestion_run_errors(phase='fetch', error_type='ReadTimeout',
                                    attempt=5, http_status=NULL)
             -> location counted in locations_failed
             -> run continues with the next location
```

### 8.5 Circuit breakers and run budgets

- **Per-run failure budget**: if `locations_failed / locations_total >
  ADCP_INGEST_FAILURE_BUDGET_RATIO` (default 50%), the run stops scheduling new
  locations, marks itself `failed`, and exits 1. This turns "the API is down" into one
  fast failed run instead of 200 slow timeouts.
- **Per-run wall clock**: `ADCP_RUN_TIMEOUT_S` (default 1h) cancels outstanding tasks;
  committed locations stay committed, in-flight ones are marked failed.
- **Per-location retry cap** guarantees the run cannot hang on one location.

### 8.6 Testing the resilience layer

- Retry policy is a pure function of exception/response -> `retry | no retry`, unit
  tested with a table of cases.
- `respx` simulates 429 with `Retry-After`, 503 then 200, timeouts, and connection
  resets.
- Backoff sleeps are patched (or `tenacity`'s `sleep` injected) so tests run in
  milliseconds while still asserting attempt counts and delays.

---

## 9. Validation rules

Validation runs in four layers. Each layer has one job, and a failure in a layer
produces a distinct, alertable `error_type`.

```
bytes -> [1 transport] -> bytes -> [2 schema] -> typed payload -> [3 domain] -> accepted rows
                                                                        |
                                                                        +-> rejected rows (+ reasons)
   [4 referential] applied before writing: location exists, source is known, units match
```

### 9.1 Layer 1 - transport

| Rule | Action on failure |
| --- | --- |
| HTTP status is 2xx | Retry if transient, else reject the location |
| `Content-Type` contains `application/json` | Reject payload (`UnexpectedContentType`) |
| Body size <= 5 MB | Reject payload (`PayloadTooLarge`) |
| Body parses as JSON | Reject payload (`MalformedJson`) |
| Response is an object, not a JSON array/error document | Reject payload (`UnexpectedShape`) |
| Provider error document (`{"error": true, "reason": "..."}`) | Reject payload with the provider's reason verbatim |

### 9.2 Layer 2 - schema (pydantic)

| Rule | Notes |
| --- | --- |
| `latitude`, `longitude`, `elevation` present and numeric | Grid cell + elevation |
| `hourly` object present | |
| `hourly.time` is a non-empty array of ISO-8601 strings | Empty array is legal but produces zero rows, not an error |
| Every requested variable key exists in `hourly` | Missing variable = provider contract change -> reject the location |
| Every variable array has `len(...) == len(hourly.time)` | Ragged arrays -> reject the location |
| Values are `number | null` (never strings) | A string where a number is expected -> reject the location |
| `hourly_units` contains the expected unit for each requested variable | Unit drift -> reject the location |
| Unit strings match the expected set exactly (`°C`, `%`, `mm`, `cm`, `hPa`, `km/h`, `iso8601`) | Normalise `%` vs `percent` before comparing |

Pydantic's `model_config = ConfigDict(extra="ignore")` on response models so new
upstream fields never break the pipeline; unknown *requested* fields are still an
error because they are explicitly enumerated.

**Implemented in the API layer (M3a).** `adcp/api/schemas.py` enforces the shape
rules - missing `hourly`, ragged arrays, wrong value types, non-finite numbers,
out-of-range location metadata - and `adcp/api/mapping.py` enforces
requested-variable presence, unit drift, timestamp parsing/localisation, and
strictly increasing hours. Every violation raises a `SchemaError` carrying the
offending field paths, is never retried, and is covered in
`tests/test_api_open_meteo.py`. Transport classification lives in
`adcp/api/open_meteo.py` (section 8.2); the row-level domain rules in 9.3 remain
the validation layer's job (M3b).

Two deliberate refinements of the M1 draft:

- `NaN`/`Infinity` are rejected at the schema boundary rather than as a row-level
  reject, because they are not valid JSON numbers in the first place - the
  validation layer never sees them.
- `weather_code` and `wind_direction_10m` must be whole numbers, because the fact
  table stores them as `smallint` and silently rounding a provider value would be
  worse than rejecting it.

**Implemented in the domain layer (M4).** `adcp/validation/rules.py` holds the
table in 9.3 as data, and `adcp/validation/validator.py` applies it to every row of
a fetched series. Accepted rows come back already normalised (UTC timestamps,
storage-scale decimals); each refused row comes back as a `Rejection` carrying its
machine-readable code, message, field paths, timestamp, location, and source.
When rejects exceed `ADCP_INGEST_MAX_INVALID_ROW_RATIO` the whole payload is
withheld - the valid rows are counted as rejected rather than written (9.6).

**One refinement: storage window versus accept range.** Open-Meteo selects whole
calendar days, so a request for "the last 72 hours" legitimately returns hours
*before* that window and the forecast tail of today *after* it. Those rows are
skipped, not rejected: `OutOfWindow` is reserved for hours outside what the request
could possibly have returned, which is a provider or planning bug. See
`adcp/pipeline/window.py` and section 9.6.

### 9.3 Layer 3 - domain rules

Applied per candidate row; violations reject the row (not the payload) unless noted.

| Field | Rule | Reject reason |
| --- | --- | --- |
| `observed_at` | parses as ISO-8601 and is timezone-aware after localisation | `InvalidTimestamp` |
| `observed_at` | minute == 0 and second == 0 (hour-aligned, with 1 s tolerance for `:59.999`) | `NotHourAligned` |
| `observed_at` | within `[window_from, window_to]` inclusive | `OutOfWindow` |
| `observed_at` | not in the future (< now + 1 h tolerance for clock skew) | `FutureTimestamp` |
| batch | no duplicate `observed_at` within the payload | `DuplicateTimestamp` |
| `temperature_2m` | -90 <= v <= 60 degC | `OutOfRange` |
| `dew_point_2m` | -90 <= v <= 60 degC | `OutOfRange` |
| `apparent_temperature` | -100 <= v <= 70 degC | `OutOfRange` |
| `relative_humidity_2m` | 0 <= v <= 100 | `OutOfRange` |
| `precipitation` / `rain` | v >= 0 | `NegativeValue` |
| `snowfall` | v >= 0 | `NegativeValue` |
| `weather_code` | integer in the WMO 4677 code set (0-99) | `InvalidWeatherCode` |
| `cloud_cover` | 0 <= v <= 100 | `OutOfRange` |
| `pressure_msl` | 800 <= v <= 1100 hPa | `OutOfRange` |
| `wind_speed_10m` / `wind_gusts_10m` | 0 <= v <= 500 km/h | `OutOfRange` |
| `wind_direction_10m` | 0 <= v <= 360 | `OutOfRange` |
| any numeric | finite (not NaN/inf) | `NonFiniteValue` |
| any field | `null` is allowed for every measurement | (not a rejection) |

Bounds are intentionally wide: they catch **provider bugs and unit changes**, not
unusual weather. A value outside them is far more likely to be a parsing error than a
record-breaking storm.

### 9.4 Layer 3b - payload-level checks

| Rule | Action |
| --- | --- |
| Returned grid cell is within 0.25 deg (approximately 25 km) of the requested point | Warning `coordinate_drift`; row still stored (implemented in M3a: `api.response.coordinate_drift`) |
| Returned elevation within +/- 500 m of any previously stored elevation for the location | Warning `elevation_drift`; stored on the row for investigation |
| Row timestamps strictly increasing | Reject payload (`NonMonotonicTimestamps`) - indicates a parsing bug (implemented in M3a as a `SchemaError`) |
| Payload overlaps the watermark by at least 1 hour on a scheduled run | Warning `watermark_discontinuity`; a genuine gap needs `backfill` |

### 9.5 Layer 4 - referential and pre-write

- `location_id` exists and `is_active` at the time of the run;
- `source` is one of the three allowed values (enforced by the enum/`CHECK` too);
- the row's `row_hash` is computed from normalised values (see §10.2) so the same
  logical row always yields the same hash - including across `-0.0`/`0.0` and
  `17.40`/`17.4`;
- the resulting row is written with `location_id` from the **database**, not from the
  response (`CoordinatesMismatch` protection against mixing up concurrent requests).

### 9.6 Rejection handling

```
rows_received  = 168
rows_accepted  = 167
rows_rejected  = 1      -> ingestion_runs.rows_rejected
                          ingestion_run_errors(phase='validate', error_type='OutOfRange',
                                               message='temperature_2m=842.0 at 2026-09-30T04:00Z')
```

- Rejected rows never reach the database, and they are never silently dropped: each
  one produces a structured log line and, when the reject ratio exceeds
  `ADCP_INGEST_MAX_INVALID_ROW_RATIO` (default 25%), the **whole payload** is rejected
  and the location is marked failed. A payload that is 30% nonsense should not
  contribute 70% data.
- The run is marked `partial` if any location has rejects but no hard failure, so
  "mostly fine" is visible in the run table rather than buried in logs.

---

## 10. Idempotency strategy

### 10.1 The natural key

```
UNIQUE (location_id, observed_at, source)
```

An hour for a location from a given source is one logical fact. Everything else about
idempotency follows from that key.

### 10.2 Content hash

```python
hashable = {
    "temperature_2m": "17.40",     # decimal-normalised, fixed scale, None -> null token
    "relative_humidity_2m": "63.00",
    ...
}
row_hash = sha256(json.dumps(hashable, sort_keys=True, separators=(",", ":"), default=str))
```

- Keys sorted, whitespace stripped -> byte-identical serialisation.
- Decimals normalised to the column scale before hashing -> `17.4` and `17.40` hash
  identically.
- Provenance fields (`run_id`, `collected_at`, `revision_count`) are **excluded** from
  the hash; the hash answers "did the measurement change?", not "did we look again?".
- Adding a new variable changes the hash **once** for every row (a full rewrite on
  first run after deploy). Mitigation: version the hash (`hash_version = 1`) so a
  change is deliberate and observable rather than accidental.

### 10.3 The upsert

```sql
INSERT INTO weather_hourly (location_id, observed_at, source, temperature_2m, ...,
                            row_hash, first_seen_run_id, last_seen_run_id)
VALUES (...), (...), ...                          -- batched with executemany / VALUES list
ON CONFLICT (location_id, observed_at, source) DO UPDATE
SET temperature_2m      = EXCLUDED.temperature_2m,
    ...,
    row_hash            = EXCLUDED.row_hash,
    last_seen_run_id    = EXCLUDED.last_seen_run_id,
    last_collected_at   = now(),
    revision_count      = weather_hourly.revision_count
                          + CASE WHEN weather_hourly.row_hash IS DISTINCT FROM EXCLUDED.row_hash
                                 THEN 1 ELSE 0 END
WHERE weather_hourly.row_hash IS DISTINCT FROM EXCLUDED.row_hash;

-- and the counter query, in the same transaction:
--   rows_inserted  = count(xmax = 0)   (true for freshly inserted tuples)
--   rows_updated   = count(xmax <> 0)  (true for rows the ON CONFLICT path rewrote)
--   rows_unchanged = rows_received - rows_inserted - rows_updated
```

Two properties matter:

- The `WHERE row_hash IS DISTINCT FROM` clause makes an unchanged row a **no-op at
  the storage layer**: no new tuple version, no WAL churn, no vacuum debt. Re-running
  an identical window costs almost nothing.
- `RETURNING (xmax = 0) AS inserted` distinguishes inserts from updates, so the run
  summary reports `inserted / updated / unchanged` honestly.

### 10.4 Transaction boundaries

```
BEGIN
  upsert batch for location L          -- one statement, all rows
  upsert ingestion_watermarks          -- advance cursor only for L
COMMIT
```

One transaction per location:

- a failure rolls back both the data and the watermark -> the next run re-requests
  the same window -> self-healing;
- locations are independent -> no cross-location rollback;
- batches stay small -> short locks, no long-running transaction holding back vacuum.

### 10.5 Concurrency safety

| Scenario | Outcome |
| --- | --- |
| Two runs at once | Advisory lock (§7.4) makes the second a no-op |
| Same run retried after a crash | Upsert makes it harmless; `revision_count` stays 0 for unchanged rows |
| Backfill overlapping a scheduled run | Row-level locking on the unique index serialises the conflicting rows; last writer wins with an identical or newer value |
| Manual rerun of an old window | Zero inserts, zero updates, zero unchanged-row writes |

### 10.6 Idempotency test matrix (automated)

| Test | Assertion |
| --- | --- |
| `test_collect_is_idempotent` | Run twice on identical fixtures -> second run reports `0 inserted, 0 updated`, table row count unchanged |
| `test_revision_detected` | Change one temperature -> `1 updated`, `revision_count = 1` |
| `test_partial_failure_rerun` | Fail location B, then rerun -> only B's rows are inserted |
| `test_concurrent_collect` | Two runs in parallel -> exactly one performs work |
| `test_crash_recovery` | Kill between fetch and commit -> watermark unchanged, rerun completes |
| `test_backfill_matches_scheduled` | Same hour via backfill and via scheduled run -> one row, `source` differs as designed |

---

## 11. Failure and partial-failure behaviour

### 11.1 Principles

1. **Fail fast on configuration.** A misconfigured run must not write anything.
2. **Isolate at the smallest sensible unit** - one location's failure never becomes
   another's.
3. **Never leave a half-written location.** Transactions are the unit of truth.
4. **Partial success is a first-class outcome**, recorded and reported, not buried.
5. **Self-healing by construction.** Because watermarks only move on commit, the next
   run repairs what the previous one missed.
6. **Exit codes are an API.** Schedulers and CI branch on them.

### 11.2 Failure catalogue

| # | Failure | Detection | System state | Exit | Operator action |
| --- | --- | --- | --- | --- | --- |
| F1 | Invalid configuration | pydantic validation at startup | Nothing written | 2 | Fix env, rerun |
| F2 | Database unreachable | DB connect timeout | Nothing written | 1 | Fix connectivity |
| F3 | Migration/schema mismatch | startup check | Nothing written | 2 | `alembic upgrade head` |
| F4 | Advisory lock held | `pg_try_advisory_lock` false | Nothing written | 0 | None (expected) |
| F5 | Upstream down for every location | all retries exhausted | Run `failed`, no rows | 1 | Check status page, rerun |
| F6 | Upstream down for some locations | failure budget not exceeded | Run `partial`, good rows committed | 3 | Rerun; next scheduled run self-heals |
| F7 | Failure budget exceeded | failed ratio > 50% | Run `failed`, remaining locations skipped | 1 | Treat as an outage |
| F8 | One payload malformed | JSON/schema error | Location failed, others fine | 3 | Inspect payload sample in errors table |
| F9 | Reject ratio over budget | rejects > 25% of rows | Location failed, no write | 3 | Investigate upstream change |
| F10 | A few out-of-range rows | domain validation | Rows rejected, rest written, run `partial` | 3 | Inspect; usually a unit change |
| F11 | DB error mid-location | exception in transaction | That location rolled back | 3 | Rerun |
| F12 | DB error mid-run (connection lost) | pool/connection error | Committed locations survive | 1 or 3 | Rerun; watermark resumes correctly |
| F13 | Process killed (SIGKILL) | orphaned `running` run row | Committed locations survive | n/a | Rerun; M6 adds a stale-run reaper |
| F14 | Run exceeds wall-clock budget | timeout cancel | In-flight locations failed | 1 | Investigate slow query/large range |
| F15 | Zero active locations | empty query | Run `skipped` | 0 | Add locations |
| F16 | Clock/watermark gap (long downtime) | window truncation check | Run `partial` + log | 3 | Re-collect with `adcp collect --lookback-hours N` (the designed `adcp backfill` is not built) |
| F17 | Disk full / DB write failure | `DiskFull`/`IntegrityError` | Rolled back location, run `failed` | 1 | Free space, rerun |

### 11.3 Run status transitions

```
                   +----------> succeeded   (all locations ok, no rejects)
                   |
running ---+------>+----------> partial     (>=1 location failed OR >=1 row rejected)
           |       |
           |       +----------> failed      (budget exceeded, or zero locations ok)
           |
           +----------> skipped             (lock held, no active locations)
```

`ingestion_runs.status` is `running` for the whole execution; a separate, periodic
reaper marks rows still `running` for longer than `2 x ADCP_RUN_TIMEOUT_S` as
`failed`. Implemented in M6 as `RunTracker.reap_stale_runs`, called at the start of
every collection inside the advisory lock, so a crashed process cannot leave a
permanently green-looking run.

### 11.6 Test coverage of the failure catalogue

Every row above is covered by an automated test or has an explicit justification
(the M6 exit criterion). "Live proof" means the scenario was also reproduced
against the running stack and recorded in the milestone report.

| # | Covering test |
| --- | --- |
| F1 | `tests/test_config.py` (validation), `tests/test_cli_errors.py` (exit 2) |
| F2 | `tests/integration/test_db_ping.py`, `test_db_cli.py`, `test_schedule_cli.py` |
| F3 | `tests/integration/test_schedule_cli_db.py::test_schedule_refuses_to_start_behind_head_schema` |
| F4 | `tests/integration/test_advisory_lock.py`, `test_pipeline_service.py::test_lock_contention_skips_the_run` |
| F5 | `tests/integration/test_pipeline_service.py::test_failure_budget_stops_the_run`, `test_collect_cli_db.py` (503 for every location) |
| F6 | `test_pipeline_service.py::test_one_failing_location_does_not_roll_back_another`, scenario Run 4 |
| F7 | `test_failure_budget_stops_the_run` (budget, run-level error row) |
| F8 | `tests/test_api_open_meteo.py` (malformed JSON, missing sections), `test_pipeline_service.py` (schema failures recorded) |
| F9 | `test_reject_budget_failure_writes_nothing`, `tests/test_validation_validator.py` (budget) |
| F10 | `test_domain_invalid_rows_are_rejected_and_recorded`, scenario Run 3/4 |
| F11, F17 | `test_write_failure_rolls_back_rows_and_watermark`, `test_a_write_failure_mid_run_keeps_committed_locations` |
| F12 | same test as F11/F17 (a write failure for one location leaves another committed) |
| F13 | `tests/integration/test_run_maintenance.py` (reaper), scenario Run 6 |
| F14 | `test_the_run_stops_at_its_wall_clock_budget` |
| F15 | `test_no_active_locations_skips_without_a_run_row` |
| F16 | `tests/test_pipeline_window.py` (truncation), `test_pipeline_service.py` (window planning) |

"Disk full" (F17) is simulated as an `OperationalError` from the write path rather
than by filling a real filesystem; the behaviour being tested - rollback of that
location, run marked failed - is identical, and filling a CI runner's disk is not a
test worth having.

### 11.4 Exit codes

| Code | Meaning | Cron/K8s interpretation |
| --- | --- | --- |
| 0 | Success, or nothing to do (lock contention, zero locations) | Healthy |
| 1 | Hard failure - the run did nothing useful | Page |
| 2 | Configuration/usage error | Page on config ownership, do not auto-retry |
| 3 | Partial success - some data written, some failed | Alert, but do not page out of hours |

### 11.5 Recovery playbook (documented in `docs/RUNBOOK.md`)

The playbook below is written against the *implemented* command set; the
`runs`/`gaps`/`backfill` commands from the original design are not built, so
their steps are done with SQL from `docs/queries.sql` instead.

1. Read the run row and its counters: query 3 in `docs/queries.sql`, or
   `SELECT * FROM ingestion_runs ORDER BY started_at DESC LIMIT 5;`
2. Read the errors for that run id: query 8 in `docs/queries.sql`.
3. Find what is actually missing: query 7 (the gap finder).
4. Repair the hole by re-collecting - idempotent by construction:
   `adcp collect --location <slug> --lookback-hours <N>`.
5. Resume normal operation: `adcp collect`, or let `adcp schedule` continue.

---

## 12. Logging and observability

### 12.1 Structured logging with `structlog`

- **JSON in production** (`ADCP_LOG_FORMAT=json`): one object per line, ready for
  Loki/CloudWatch/ELK, with no regex parsing.
- **Console renderer for local development**: colourised, aligned key-values, still
  the same event names and fields.
- **stdlib integration**: `structlog.stdlib.ProcessorFormatter` means `httpx`,
  `asyncio`, and any third-party logger emit through the same pipeline with the same
  formatter - no second log format in mixed output.
- **Context binding**: `structlog.contextvars.bind_contextvars(run_id=..., env=...)`
  means every line inside a run carries the run id without being passed around.
  Implemented in M6: the service binds `run_id` for the whole collection and
  `location`/`source` per location, and each worker task runs in a *copy* of that
  context (`contextvars.copy_context`), so pool threads carry the same correlation
  fields as the main thread.
- **Redaction order**: the redactor runs *after* the traceback and stack are
  rendered into strings. Run earlier it would only see an `exc_info` tuple, and an
  exception message quoting a DSN or API key would sail straight through - a real
  leak the M6 audit found and fixed.
- **Idempotent configuration**: `configure_logging()` is safe to call twice (CLI +
  tests + library use).

```json
{"event":"ingest.location.completed","level":"info","timestamp":"2026-10-02T09:07:41.512Z",
 "service":"adcp","env":"prod","app_version":"0.4.0","run_id":"6f1d...","location":"belgrade-rs",
 "source":"forecast","rows_received":168,"rows_inserted":24,"rows_updated":0,"rows_unchanged":144,
 "rows_rejected":0,"duration_ms":412,"attempts":1}
```

### 12.2 Event naming convention

`<area>.<subject>.<verb-past-tense>` - stable, greppable, and useful as a metric key:

| Event | Level | Meaning |
| --- | --- | --- |
| `app.startup` | info | Process started, version + resolved config summary (secrets masked) |
| `config.loaded` | info | Config validated |
| `db.connected` | info | Connection established |
| `ingest.run.started` | info | Run row created, window computed |
| `ingest.run.skipped` | warning | Lock held or no locations |
| `ingest.location.started` | info | Fetch beginning |
| `ingest.location.completed` | info | Rows written, counters |
| `ingest.location.failed` | error | Location abandoned, reason + attempt count |
| `ingest.row.rejected` | warning | Single row rejected with reason |
| `ingest.retry.scheduled` | warning | Attempt N failed; sleeping T seconds |
| `ingest.run.completed` | info | Terminal status + full counter digest |
| `ingest.run.failed` | error | Run failed, error summary |
| `db.query.slow` | warning | Statement over `ADCP_DB_SLOW_QUERY_MS` (implemented M6) |
| `cli.unexpected_error` | error | A bug reached the top of the CLI; logged in full, reported as one line |
| `ingest.runs.reaped` | warning | A run left `running` by a killed process was failed |
| `scheduler.*` | info/warning/error | Scheduler lifecycle (section 7.7) |

### 12.3 Log level policy

| Level | Use for | Examples |
| --- | --- | --- |
| DEBUG | Per-row detail; disabled in prod | individual row payloads, SQL text |
| INFO | State transitions, run summaries, one line per location | `ingest.location.completed` |
| WARNING | Degraded but handled: retries, rejects, drift, skips | `ingest.retry.scheduled` |
| ERROR | Failed unit of work requiring attention | `ingest.location.failed` |
| CRITICAL | Pipeline cannot function at all | `db.connection_lost`, budget exceeded |

### 12.4 What must never be logged

Passwords, `DATABASE_URL` with credentials, API keys, full upstream response bodies at
INFO, or raw config dumps. Enforced by:

- a `redact_secrets` structlog processor that masks any key in
  `{password, dsn, database_url, api_key, token, authorization}` and masks the
  password component of any URL-like value;
- `Settings.masked_database_url()` (already implemented in M1) for the config summary;
- CLI output that shows hashes, not secrets.

### 12.5 Run tracking as the operational source of truth

Logs are for investigation; the database is for reporting. Every run is queryable:

```sql
-- last 24 runs at a glance
SELECT started_at, status, locations_succeeded || '/' || locations_total AS locations,
       rows_inserted, rows_updated, rows_rejected, duration_ms
FROM ingestion_runs ORDER BY started_at DESC LIMIT 24;

-- freshness per location (is anything stale?)
SELECT l.slug, w.source, max(w.observed_at) AS newest, now() - max(w.observed_at) AS lag
FROM locations l LEFT JOIN weather_hourly w ON w.location_id = l.id
WHERE l.is_active GROUP BY l.slug, w.source ORDER BY lag DESC NULLS LAST;

-- which error types are trending?
SELECT error_type, count(*) FROM ingestion_run_errors
WHERE occurred_at > now() - interval '7 days' GROUP BY 1 ORDER BY 2 DESC;

-- forecast revision churn
SELECT location_id, count(*) FROM weather_hourly WHERE revision_count > 0 GROUP BY 1;
```

### 12.6 Health and alerting signals

| Signal | Query | Alert threshold (documented) |
| --- | --- | --- |
| Freshness | newest `observed_at` per active location | > 3 hours stale |
| Run success rate | `status='succeeded'` share over 24h | < 90% |
| Partial runs | `status='partial'` count over 24h | > 2 |
| Zero-run detection | max(`started_at`) | > 2 hours ago |
| Validation rejects | `rows_rejected` sum over 24h | > 100 |
| Stale `running` run | `finished_at IS NULL AND started_at < now() - 2h` | any |

A designed `adcp health` command would print these as a table and return a
non-zero exit code when a threshold is breached, so it could act as a container
healthcheck. It is **not built** (section 17); the queries above and
`docs/queries.sql` answer the same questions today. The
`ADCP_HEALTHCHECK_FRESHNESS_HOURS` setting exists and is validated, but nothing
consumes it yet.

### 12.7 Metrics (optional, M6)

If a Prometheus exporter is desired, the same counters already computed for
`ingestion_runs` are exported as `adcp_rows_inserted_total{source=...}`,
`adcp_run_duration_seconds`, `adcp_location_failures_total{error_type=...}`. Deliberately
deferred: the run tables already provide the operational truth without a new
dependency.

---

## 13. CLI design

### 13.1 Principles

- One binary (`adcp`), subcommands grouped by noun (`config`, `db`, `runs`).
- Human-readable by default; `--json` for scripting.
- Mutating commands offer a safe preview where one is meaningful:
  `adcp collect --dry-run` writes nothing, and `adcp db prune` reports by
  default and deletes only with `--apply`.
- Exit codes are the contract from §11.4.
- No ingestion logic in the CLI layer - it composes settings, adapters, and the
  service, then maps the result to an exit code.
- Implemented with `typer` (type hints drive parsing and help text).

### 13.2 Command tree

`[x]` = implemented and tested; `[ ]` = designed in this document but **not
built** (see section 17, "Deliberately not built").

```
adcp
|-- --version                       [x]  same as `adcp version`
|-- config
|   |-- show [--json]               [x]  resolved config, secrets masked
|   |-- check                       [x]  validate settings, no I/O
|   `-- init [--force]              [ ]  write .env from the template
|-- db
|   |-- ping                        [x]  connectivity + server version
|   |-- upgrade [--revision head]   [x]  alembic upgrade
|   |-- current [--check]           [x]  schema revision
|   |-- prune --runs-older-than-days [x] retention, dry-run default
|   |-- stats                       [ ]  row counts, table sizes
|   `-- gaps --location --from --to [ ]  missing hours finder (query 7 covers it today)
|-- locations
|   |-- list [--json]               [ ]  `scripts/seed_locations.py` covers the demo path
|   |-- add --slug --lat --lon ...  [ ]
|   |-- disable --slug / enable     [ ]
|   `-- import --file locations.yaml[ ]
|-- collect                         [x]
|   |-- location <slug>...          filter (repeatable)
|   |-- lookback-hours N            override ADCP_INGEST_LOOKBACK_HOURS
|   |-- overlap-hours N             override ADCP_INGEST_OVERLAP_HOURS
|   |-- dry-run                     fetch + validate, write nothing, report counts
|   `-- json                        machine-readable run summary
|-- backfill                        [ ]
|   |-- from <date>  --to <date>
|   |-- location <slug>...
|   `-- chunk-days N                default 31
|-- runs
|   |-- list [--limit N] [--status] [ ]  query 3 in `docs/queries.sql` today
|   |-- show <run_id> [--json]      [ ]
|   `-- errors <run_id> [--limit N] [ ]
|-- schedule                        [x]  long-running APScheduler process
|   |-- minute N  --timezone TZ
|   |-- interval-seconds N          development cadence
|   `-- run-once                    execute immediately, then schedule
|-- health                          [ ]  freshness thresholds -> exit code (query 1 covers it today)
`-- version                         [x]
```

### 13.3 Examples

```bash
# Validate configuration before deploying
adcp config check && echo "config ok"

# Inspect what the process will actually use, with the password masked
adcp config show --json

# Safe first contact with the API: fetch and validate, write nothing
adcp collect --dry-run --location belgrade-rs

# Normal scheduled operation
adcp collect

# Recent-run forensics (designed command - not built; use docs/queries.sql today)
# adcp runs list --limit 10
# adcp runs show 6f1d9c1e-... --json
# adcp runs errors 6f1d9c1e-... --limit 20

# Repair a gap (designed command - not built; widen the lookback instead)
adcp collect --location belgrade-rs --lookback-hours 168
```

### 13.4 Exit-code mapping in code

```python
class ExitCode(IntEnum):
    OK = 0
    FAILURE = 1
    CONFIG_ERROR = 2
    PARTIAL = 3
```

`adcp collect` maps the returned `RunSummary` onto these codes
(`adcp.cli.collect_cmd.exit_code_for`): `succeeded`/`skipped` -> 0, `partial` -> 3,
`failed` -> 1, configuration/usage problems -> 2. Typer's `raise typer.Exit(code)`
is used so no tracebacks leak for expected failures.

### 13.5 CLI implemented (M7)

```bash
adcp --version           # adcp 0.1.0
adcp version
adcp config show         # resolved settings, database URL password masked
adcp config show --json
adcp config check        # exit 0 when valid, exit 2 with a readable list when not
adcp db ping             # exit 0 reachable, 1 unreachable, 2 invalid config
adcp db ping --json
adcp db upgrade          # applies pending migrations (--revision to target one)
adcp db upgrade --json
adcp db current          # applied revision vs head, exit 1 with --check when behind
adcp db prune            # retention report; --apply deletes

adcp collect             # one collection run: fetch, validate, upsert, watermark
adcp collect --location belgrade-rs --lookback-hours 48 --overlap-hours 6
adcp collect --dry-run   # fetch and validate only; writes nothing at all
adcp collect --json      # machine-readable run summary on stdout

adcp schedule                                  # hourly at :07 UTC, until interrupted
adcp schedule --minute 15 --timezone Europe/Belgrade
adcp schedule --interval-seconds 15 --run-once  # development/demo cadence
```

`db` command rules:

- `--json` writes exactly one JSON document and nothing else, so it is safe to pipe;
- the connection string is always rendered through `masked_database_url()`;
- expected failures (unreachable database, invalid configuration) exit with the
  documented code and a single-line message - never a traceback.

---

## 14. Testing strategy

### 14.1 Test pyramid

```
        /\
       /  \      contract/e2e   (few)  recorded Open-Meteo fixtures -> real PostgreSQL
      /----\
     /      \    integration    (some) respx-mocked HTTP + containerised PostgreSQL
    /--------\
   /          \  unit           (many) validation rules, retry policy, window maths,
  /------------\                        hashing, config, logging, CLI wiring
```

### 14.2 Layers

| Layer | Marker | Tools | Scope | Target |
| --- | --- | --- | --- | --- |
| Unit | `@pytest.mark.unit` | pytest, `pytest.approx` | Pure functions: window planning, `row_hash`, validation rules, backoff computation, settings precedence, log rendering | Fast (< 5 s total), no I/O |
| Integration (HTTP) | `@pytest.mark.integration` | `respx` | Client construction, retry/backoff behaviour, 429 `Retry-After`, timeouts, malformed payloads, error mapping | No real network |
| Integration (DB) | `@pytest.mark.integration` | `testcontainers-postgres` (fallback: `docker compose up postgres`) | Migrations, upsert semantics, `xmax` insert/update counting, watermark transactions, constraint enforcement, rollback behaviour | Real PostgreSQL, never SQLite |
| Contract | `@pytest.mark.integration` | stored JSON fixtures + optional live smoke test | The real response shape still maps to the models; detects upstream schema drift | Fixtures refreshed deliberately |
| CLI | `@pytest.mark.unit` | `typer.testing.CliRunner` | Command registration, exit codes, output formatting, secret masking | |
| End-to-end | `@pytest.mark.slow` (opt-in) | compose stack + real API | One real location, one real hour, full path | Manual/nightly only |

**Never test the database against SQLite.** `ON CONFLICT ... WHERE`, `xmax`, `numeric`
semantics, advisory locks, and `timestamptz` behaviour are all PostgreSQL-specific;
testing against SQLite would validate the wrong engine.

### 14.3 What each milestone must prove

| Milestone | Required tests |
| --- | --- |
| M1 | Settings defaults/overrides/validation, secret masking, logging renders JSON + console, CLI version/config commands, `.env.example` keys match `Settings` fields, compose file is valid YAML |
| M2 | Migrations apply and roll back (from empty, `-1`, and `base`); Core metadata matches the migrated schema; constraints reject bad data (duplicate natural key, bad latitude, non-hour-aligned timestamp, unknown source); `db ping` fails cleanly without a database; advisory-lock mutual exclusion, release on exit/exception, wait-for-release, and session-level (not transaction-level) semantics; credentials never appear in CLI output or logs |
| M3 | Response parsing against fixtures; every schema/domain rule has a positive and negative case; retry policy table; `--dry-run` writes nothing |
| M4 | Idempotency matrix from §10.6; watermark monotonicity; partial-failure isolation; run counter accuracy; crash recovery |
| M5 | Scheduler job config; advisory lock contention; backfill chunking; `health` thresholds; `db gaps` correctness |
| M6 | Log redaction; retention pruning with `--dry-run`; stale-run reaper |

### 14.4 Fixtures

```
tests/fixtures/open_meteo/
  forecast_single_location.json            recorded: 72 hours x 13 variables (200 OK)
  archive_date_range.json                  recorded: 3-day archive range (200 OK)
  forecast_with_nulls.json                 derived: sparse measurements are legal
  forecast_ragged_arrays.json              derived: one array is short
  forecast_missing_variable.json           derived: wind_gusts_10m removed
  forecast_missing_hourly.json             derived: no hourly block at all
  forecast_wrong_field_type.json           derived: "17.4" string in a numeric array
  forecast_units_changed.json              derived: wind in m/s instead of km/h
  forecast_invalid_timestamp.json          derived: unparseable time string
  forecast_non_monotonic.json              derived: hours swapped out of order
  forecast_invalid_location_metadata.json  derived: latitude 999
  error_invalid_coordinates.json           recorded: HTTP 400 with a provider reason
  error_provider_error_document.json       provider error document served with 200
  error_rate_limited.json                  429 with Retry-After: 42
  error_malformed_json.json                deliberately truncated JSON text
```

The two "recorded" fixtures come from live calls made by
`scripts/record_open_meteo_fixtures.py`; every "derived" fixture is produced by
mutating the recorded payload inside that same script, so each one breaks exactly
one documented thing. Re-recording is a reviewable diff, which is how provider
contract changes are noticed (Appendix A1/A2 were confirmed this way: with
`timezone=UTC` the provider returns naive timestamps and the unit spellings now
asserted in `OPEN_METEO_UNITS`).

### 14.5 Quality gates

| Gate | Tool | Threshold |
| --- | --- | --- |
| Lint | `ruff check` | Zero findings |
| Format | `ruff format --check` | Zero diffs |
| Types | `mypy` (strict) | Zero errors, `src` fully annotated |
| Tests | `pytest` | All pass |
| Coverage | `pytest --cov` | >= 85% line and branch on `src/adcp` |
| Migration drift | `alembic check` (M2+), also asserted by `tests/integration/test_migrations.py` | Clean |
| Compose validity | `docker compose config -q` | Exit 0 |

CI runs the gates on Python 3.12 and 3.13 (and 3.14 once the ecosystem wheels are
universally available) on every push and pull request, with a PostgreSQL service
container for the integration tests.

### 14.6 Determinism rules

- No `time.sleep` in tests: inject a clock (`Clock` protocol with `now()`) and patch
  `tenacity`'s sleep.
- No real network in unit/integration tests: `respx` raises on any unmocked request.
- Fixed random seed for property-based tests (`hypothesis`).
- Tests never share database state: each integration test runs in a transaction that
  is rolled back, or uses a uniquely named schema.
- `Settings` is constructed with `_env_file=None` in tests so a developer's local
  `.env` cannot change results.

### 14.7 M1 tests (the original scaffold suite)

Only scaffold behaviour that actually exists:

- `tests/test_package.py` - package imports, `__version__` present, `py.typed` shipped;
- `tests/test_config.py` - defaults, env overrides, alias handling, validation errors,
  `masked_database_url()` never leaks the password;
- `tests/test_logging.py` - JSON output is parseable and contains bound context;
  console output is human-readable; levels are respected; secrets are redacted;
- `tests/test_cli.py` - `--version`, `config show` (including masking), `config check`
  exit codes;
- `tests/test_project_layout.py` - required files exist, `.env.example` covers every
  `Settings` field, `docker-compose.yml` is valid YAML with a `postgres` service.

---

## 15. Configuration/environment variables

### 15.1 Precedence

```
CLI flag  >  process environment  >  .env file  >  code defaults
```

Implemented with `pydantic-settings`: `env_prefix="ADCP_"`,
`env_nested_delimiter="__"`, `env_file=".env"`, `case_sensitive=False`,
`extra="ignore"`. `get_settings()` is `lru_cache`d so the process reads the
environment once (tests bypass the cache explicitly).

### 15.2 Rules

- **Validate at startup, fail fast.** An invalid value aborts before any network or
  database work, with every error listed at once (exit code 2), not one per run.
- **No secrets in logs.** `Settings.masked_database_url()` is the only way config
  summary output renders the DSN.
- **No magic numbers in code.** Every tunable lives here, is documented in
  `.env.example`, and is covered by a test that keeps the two in sync.
- **`.env` is git-ignored; `.env.example` is committed.** `.env.example` must be
  runnable as-is for local development.
- **Production deployments inject environment variables** (systemd `Environment=`,
  Compose `env_file`, Kubernetes `Secret`/`ConfigMap`). `.env` files are a local
  convenience, not a production mechanism.

### 15.3 Reference

**Application**

| Variable | Type | Default | Required | Notes |
| --- | --- | --- | --- | --- |
| `ADCP_ENV` | `local \| dev \| staging \| prod` | `local` | no | Drives log format defaults and guard rails |
| `ADCP_SERVICE_NAME` | str | `adcp` | no | Appears in every log record |
| `ADCP_RUN_TIMEOUT_S` | int > 0 | `3600` | no | Wall-clock budget for a whole run |
| `ADCP_HEALTHCHECK_FRESHNESS_HOURS` | int > 0 | `3` | no | Reserved for the designed (not built) `adcp health`; validated but not consumed |

**Logging**

| Variable | Type | Default | Required | Notes |
| --- | --- | --- | --- | --- |
| `ADCP_LOG_LEVEL` | `DEBUG\|INFO\|WARNING\|ERROR\|CRITICAL` | `INFO` | no | |
| `ADCP_LOG_FORMAT` | `json \| console \| auto` | `console` | no | `auto` = JSON when stdout is not a TTY |
| `ADCP_LOG_INCLUDE_CALLER` | bool | `false` | no | Adds module:line to each record |

**Database**

| Variable | Type | Default | Required | Notes |
| --- | --- | --- | --- | --- |
| `ADCP_DATABASE_URL` / `DATABASE_URL` | `postgresql+psycopg://...` | local dev DSN | yes (M2+) | Alias accepted for platform compatibility; password masked in output |
| `ADCP_DB_POOL_MIN_SIZE` | int >= 0 | `1` | no | SQLAlchemy `pool_size`: connections kept at steady state |
| `ADCP_DB_POOL_MAX_SIZE` | int >= 1 | `5` | no | Hard ceiling, implemented as `pool_size + max_overflow`; must be >= min size |
| `ADCP_DB_CONNECT_TIMEOUT_S` | int > 0 | `5` | no | |
| `ADCP_DB_STATEMENT_TIMEOUT_MS` | int > 0 | `30000` | no | Applied via `SET statement_timeout` |
| `ADCP_DB_SLOW_QUERY_MS` | int > 0 | `1000` | no | Threshold for `db.query.slow` |

**Open-Meteo**

| Variable | Type | Default | Required | Notes |
| --- | --- | --- | --- | --- |
| `ADCP_OPEN_METEO_FORECAST_URL` | URL | `https://api.open-meteo.com/v1/forecast` | no | |
| `ADCP_OPEN_METEO_ARCHIVE_URL` | URL | `https://archive-api.open-meteo.com/v1/archive` | no | |
| `ADCP_OPEN_METEO_HISTORICAL_FORECAST_URL` | URL | `https://historical-forecast-api.open-meteo.com/v1/forecast` | no | |
| `ADCP_OPEN_METEO_TIMEOUT_CONNECT_S` | float > 0 | `5` | no | |
| `ADCP_OPEN_METEO_TIMEOUT_READ_S` | float > 0 | `20` | no | |
| `ADCP_OPEN_METEO_TIMEOUT_WRITE_S` | float > 0 | `10` | no | |
| `ADCP_OPEN_METEO_TIMEOUT_TOTAL_S` | float > 0 | `60` | no | Whole-request budget covering every attempt and its backoff |
| `ADCP_OPEN_METEO_MAX_ATTEMPTS` | int 1..10 | `5` | no | Retries counted after the first attempt |
| `ADCP_OPEN_METEO_BACKOFF_INITIAL_S` | float > 0 | `1` | no | |
| `ADCP_OPEN_METEO_BACKOFF_MAX_S` | float > 0 | `30` | no | Must be >= initial |
| `ADCP_OPEN_METEO_MAX_CONCURRENCY` | int 1..32 | `4` | no | Locations fetched in parallel |
| `ADCP_OPEN_METEO_CHUNK_PAUSE_S` | float >= 0 | `0.5` | no | Reserved for the designed (not built) backfill chunk pause; validated but not consumed |
| `ADCP_OPEN_METEO_USER_AGENT` | str | `adcp/<version> (+repo)` | no | Sent on every request |
| `ADCP_OPEN_METEO_API_KEY` | secret | unset | no | Only for a commercial key; never logged |

**Ingestion**

| Variable | Type | Default | Required | Notes |
| --- | --- | --- | --- | --- |
| `ADCP_INGEST_LOOKBACK_HOURS` | int 1..2160 | `72` | no | Must stay within the upstream `past_days` limit |
| `ADCP_INGEST_OVERLAP_HOURS` | int 0..168 | `24` | no | Revision window |
| `ADCP_INGEST_FAILURE_BUDGET_RATIO` | float 0..1 | `0.5` | no | Run aborts above this failure share |
| `ADCP_INGEST_MAX_INVALID_ROW_RATIO` | float 0..1 | `0.25` | no | Payload rejected above this reject share |
| `ADCP_LOCATIONS_FILE` | path | unset | no | Reserved: YAML location list. Validated but not yet consumed - locations come from the database (`scripts/seed_locations.py` for the demo) |

**Scheduler**

| Variable | Type | Default | Required | Notes |
| --- | --- | --- | --- | --- |
| `ADCP_SCHEDULER_ENABLED` | bool | `false` | no | |
| `ADCP_SCHEDULER_MINUTE` | int 0..59 | `7` | no | Minute past the hour |
| `ADCP_SCHEDULER_TIMEZONE` | IANA tz | `UTC` | no | Validated against `zoneinfo` |
| `ADCP_SCHEDULER_SKIP_IF_RUNNING` | bool | `true` | no | |
| `ADCP_SCHEDULER_INTERVAL_SECONDS` | int >= 0 | `0` | no | `0` keeps the hourly schedule; a positive value collects every N seconds (development/demo cadence) |

**Compose-only (consumed by `docker-compose.yml`, not by `Settings`)**

| Variable | Default | Notes |
| --- | --- | --- |
| `POSTGRES_USER` | `adcp` | Must match the DSN in `ADCP_DATABASE_URL` |
| `POSTGRES_PASSWORD` | `adcp_local_dev` | Local development only |
| `POSTGRES_DB` | `adcp` | |
| `POSTGRES_PORT` | `55432` | Published host port; the container itself always listens on 5432, so a host PostgreSQL is not disturbed |

**Test-only variables** (read by the test suite, never by `Settings`):

| Variable | Default | Purpose |
| --- | --- | --- |
| `ADCP_TEST_DATABASE_URL` | unset | Point integration tests at a disposable database (CI service container, or a database name containing `test`) |
| `ADCP_TEST_USE_TESTCONTAINERS` | `true` | Set to `false` to skip the Testcontainers option and use the Compose fallback |
| `ADCP_TEST_ALLOW_ANY_DATABASE` | `false` | Override the guard that refuses to truncate a database whose name does not contain `test` |

### 15.4 Validation examples

| Input | Result |
| --- | --- |
| `ADCP_LOG_FORMAT=yaml` | Exit 2: `log_format: Input should be 'json', 'console' or 'auto'` |
| `ADCP_DB_POOL_MAX_SIZE=0` | Exit 2: `db_pool_max_size: Input should be greater than or equal to 1` |
| `ADCP_DB_POOL_MIN_SIZE=10` with max `5` | Exit 2: `db_pool_max_size must be >= db_pool_min_size` |
| `ADCP_INGEST_OVERLAP_HOURS=200` | Exit 2: `overlap must be <= 168 hours (7 days)` |
| `ADCP_SCHEDULER_TIMEZONE=Europe/Nowhere` | Exit 2: `unknown timezone` |
| `DATABASE_URL=not-a-url` | Exit 2: `database_url: Input should be a valid URL` |

Cross-field checks are implemented as `@model_validator(mode="after")` in
`adcp.config` and are unit tested.

### 15.5 Production guards

Two silent-default mistakes are refused outright when `ADCP_ENV=prod`, because
neither is a typo - it is what an inherited local configuration looks like:

| Guard | Message |
| --- | --- |
| The database URL is still the shipped local-development default | `ADCP_DATABASE_URL is still the shipped local-development default ...; set it to the production database` |
| Logs would be written in console format | `ADCP_LOG_FORMAT must be 'json' (or 'auto') when ADCP_ENV=prod` |

The container image already sets `ADCP_ENV=prod` with JSON logs, and Compose
overrides the environment to `local` for development, so the guards only fire when
a deployment genuinely forgot to configure itself.

---

## 16. Repository structure

### 16.1 Repository layout (M7)

```
.
|-- .dockerignore
|-- .env.example                 # every setting, documented, runnable as-is
|-- .github/
|   `-- workflows/ci.yml         # lint, types, tests, compose validation, migrations
|-- .gitignore
|-- Dockerfile                   # multi-stage, non-root, healthcheck
|-- LICENSE
|-- README.md
|-- alembic.ini                  # for the `alembic` CLI; DSN comes from settings
|-- docker-compose.yml           # postgres (+ tools/adminer, app profiles)
|-- docs/
|   |-- PLAN.md                  # this document
|   |-- ARCHITECTURE.md          # Mermaid: components, data flow, ER, retry, states
|   |-- RUNBOOK.md               # operator procedures
|   |-- DEMO.md                  # narrated 3-5 minute demonstration
|   |-- CASE_STUDY.md            # problem -> solution -> measured results
|   |-- PORTFOLIO.md             # the client-facing explanation
|   |-- queries.sql              # runnable SQL analytics pack
|   `-- samples/                 # deterministic captured command output
|-- pyproject.toml               # metadata, deps, ruff/mypy/pytest/coverage config
|-- uv.lock                      # committed lockfile (uv lock)
|-- scripts/
|   |-- capture_samples.py             # regenerate docs/samples (offline, deterministic)
|   |-- demo.py                        # end-to-end walkthrough of the README commands
|   |-- load_check.py                  # opt-in 50-location sanity check
|   |-- record_open_meteo_fixtures.py  # live fixture recorder (dev tool)
|   |-- scheduler_demo.py              # run a few cycles, stop cleanly, verify
|   `-- seed_locations.py              # demo locations, idempotent
|-- src/
|   `-- adcp/
|       |-- __init__.py          # __version__, package metadata
|       |-- __main__.py          # python -m adcp
|       |-- api/
|       |   |-- __init__.py
|       |   |-- mapping.py       # payload -> domain types, structured SchemaErrors
|       |   |-- open_meteo.py    # HTTP adapter: requests, retries, logging
|       |   |-- requests.py      # deterministic request construction
|       |   `-- schemas.py       # strict wire models + expected units
|       |-- cli/
|       |   |-- __init__.py      # app assembly: registers every command group
|       |   |-- common.py        # shared CLI helpers (settings, output, errors)
|       |   |-- collect_cmd.py   # `adcp collect`: exit codes, JSON, dry-run
|       |   |-- schedule_cmd.py  # `adcp schedule`: preflight + long-running loop
|       |   |-- config_cmd.py    # config show | check
|       |   |-- db_cmd.py        # db ping | upgrade | current
|       |   `-- main.py          # root app, root callback, version
|       |-- config.py            # pydantic-settings Settings + get_settings()
|       |-- db/
|       |   |-- __init__.py
|       |   |-- engine.py        # engine/pool, connection scope, ping
|       |   |-- lock.py          # PostgreSQL advisory lock
|       |   |-- repository.py    # location + weather repositories (upsert, counters)
|       |   |-- run_tracker.py   # run lifecycle, counters, errors, stale-run reaper
|       |   |-- tables.py        # Core table definitions = the schema in code
|       |   |-- watermark_store.py
|       |   `-- migrations/
|       |       |-- env.py       # Alembic environment
|       |       |-- runner.py    # programmatic upgrade/current/downgrade
|       |       |-- script.py.mako
|       |       `-- versions/    # 0001..0005, one per table
|       |-- errors.py            # AdcpError + database/upstream/schema error types
|       |-- exit_codes.py        # ExitCode IntEnum (the process contract)
|       |-- logging.py           # structlog configuration + secret redaction
|       |-- models/
|       |   |-- location.py      # Location + LocationLike protocol
|       |   |-- observation.py   # ObservationSource, WeatherObservation, WeatherSeries
|       |   |-- run.py           # RunStatus, LocationResult, RunCounts, RunSummary
|       |   `-- window.py        # DateRange / RecentWindow
|       |-- normalization.py     # canonical values, quantisation, row_hash
|       |-- pipeline/
|       |   |-- __init__.py
|       |   |-- service.py       # CollectionService: one run, start to finish
|       |   `-- window.py        # watermark -> storage window + accept range
|       |-- ports.py             # WeatherSource protocol
|       |-- resilience.py        # timeout/retry/backoff policy
|       |-- scheduler.py         # APScheduler wrapper: plan, job, shutdown
|       |-- validation/
|       |   |-- __init__.py
|       |   |-- rules.py         # section 9.3 rules as data + Rejection
|       |   `-- validator.py     # series -> accepted rows + rejections
|       `-- py.typed             # PEP 561 marker
`-- tests/
    |-- __init__.py
    |-- conftest.py              # environment isolation + Settings factory fixtures
    |-- fixtures/open_meteo/     # recorded + derived payloads
    |-- support.py               # fixture loaders and log helpers
    |-- integration/
    |   |-- conftest.py          # testcontainers | compose | ADCP_TEST_DATABASE_URL
    |   |-- test_advisory_lock.py
    |   |-- test_api_with_database_locations.py
    |   |-- test_collect_cli_db.py
    |   |-- test_db_cli.py
    |   |-- test_db_observability.py
    |   |-- test_db_ping.py
    |   |-- test_end_to_end_scenario.py
    |   |-- test_migrations.py
    |   |-- test_pipeline_service.py
    |   |-- test_repositories.py
    |   |-- test_run_maintenance.py
    |   |-- test_run_tracker_accounting.py
    |   |-- test_schedule_cli_db.py
    |   |-- test_watermark_store.py
    |   |-- test_weather_repository.py
    |   `-- test_schema_constraints.py
    |-- test_db_engine.py
    |-- test_docs.py            # link/anchor integrity, no machine paths, CLI coverage
    |-- test_api_models.py
    |-- test_api_open_meteo.py
    |-- test_api_requests.py
    |-- test_api_schemas.py
    |-- test_architecture.py
    |-- test_cli.py
    |-- test_cli_errors.py
    |-- test_collect_cli.py
    |-- test_config.py
    |-- test_errors.py
    |-- test_live_open_meteo.py  # opt-in: ADCP_LIVE_API_TESTS=true
    |-- test_logging.py
    |-- test_migrations_metadata.py
    |-- test_normalization.py
    |-- test_package.py
    |-- test_pipeline_status.py
    |-- test_pipeline_window.py
    |-- test_resilience.py
    |-- test_schedule_cli.py
    |-- test_scheduler.py
    |-- test_validation_rules.py
    |-- test_validation_validator.py
    `-- test_project_layout.py
```

Integration fixtures live in ``tests/integration/conftest.py`` rather than the root
``conftest.py``: only that directory needs Testcontainers, so unit tests stay fast
and keep working on a machine with no Docker.

### 16.2 Original target structure (partially built)

Kept for reference. Deltas from the real tree in 16.1: `cli/runs_cmd.py`,
`cli/health_cmd.py`, `pipeline/backfill.py`, `clock.py`, `tests/unit/`,
`tests/e2e/`, `docs/adr/`, and `scripts/demo.ps1|sh` were designed but not
built; `docs/ARCHITECTURE.md`, `docs/RUNBOOK.md`, `docs/DEMO.md`,
`docs/CASE_STUDY.md`, `docs/PORTFOLIO.md`, `docs/queries.sql`, and
`docs/samples/` were added instead.

```
src/adcp/
|-- __init__.py, __main__.py, py.typed
|-- cli/
|   |-- __init__.py              # app assembly
|   |-- common.py                # shared helpers: settings, output, exit codes
|   |-- main.py                  # adcp, version
|   |-- config_cmd.py
|   |-- db_cmd.py
|   |-- collect_cmd.py
|   |-- runs_cmd.py
|   |-- schedule_cmd.py
|   `-- health_cmd.py
|-- config.py                    # Settings, get_settings()
|-- logging.py                   # configure_logging, get_logger, redaction
|-- errors.py                    # AdcpError + database types (M2); upstream/schema types land in M3
|-- exit_codes.py
|-- clock.py                     # Clock protocol + SystemClock (testable time)
|-- models/
|   |-- __init__.py
|   |-- location.py
|   |-- observation.py           # WeatherObservation (domain, hashable)
|   `-- run.py                   # RunSummary, RunStatus, LocationResult
|-- ports.py                     # WeatherSource, WeatherRepository, RunTracker, WatermarkStore
|-- api/
|   |-- __init__.py
|   |-- open_meteo.py            # client + request building
|   `-- schemas.py               # pydantic response models
|-- validation/
|   |-- __init__.py
|   |-- rules.py                 # domain range rules
|   `-- validator.py             # payload -> accepted/rejected rows
|-- resilience.py                # retry predicate, backoff, httpx timeout factory
|-- db/
|   |-- __init__.py
|   |-- engine.py                # engine/pool creation, connection scope, ping
|   |-- tables.py                # Core table definitions (the schema in code)
|   |-- repository.py            # locations + weather upserts
|   |-- run_tracker.py           # ingestion_runs / ingestion_run_errors
|   |-- watermark_store.py       # per-location/source cursors
|   |-- lock.py                  # advisory lock context manager
|   `-- migrations/              # env.py, runner.py, script.py.mako, versions/
|-- pipeline/
|   |-- __init__.py
|   |-- service.py               # IngestionService (orchestration)
|   |-- window.py                # window planning (watermark -> [from, to])
|   `-- backfill.py
`-- scheduler.py                 # APScheduler wiring

docs/
|-- PLAN.md
|-- ARCHITECTURE.md              # diagrams exported from this plan (M7)
|-- RUNBOOK.md                   # incident playbook (M6)
|-- adr/0001-idempotent-upserts.md ... (M7)
`-- queries.sql                  # the demo/analytics queries (M4+)

tests/
|-- conftest.py                  # fixtures: settings factory, db, respx, clock
|-- fixtures/open_meteo/*.json
|-- unit/ ...
|-- integration/ ...
`-- e2e/test_live_smoke.py       # opt-in, real API

scripts/
|-- demo.ps1 / demo.sh           # the five-minute portfolio demo (M7)
`-- seed_locations.py
```

### 16.3 Naming conventions

| Element | Convention | Example |
| --- | --- | --- |
| Package/module | `snake_case`, plural for collections | `db.repository`, `models` |
| Class | `PascalCase` | `IngestionService`, `OpenMeteoClient` |
| Function/method | `snake_case`, verbs | `plan_window`, `upsert_observations` |
| Constant | `UPPER_SNAKE` | `DEFAULT_TIMEOUT_S` |
| DB table | `snake_case`, plural | `weather_hourly`, `ingestion_runs` |
| DB column | `snake_case`, explicit units | `temperature_2m`, `elevation_m` |
| Log event | `area.subject.past_tense_verb` | `ingest.location.completed` |
| Env var | `ADCP_UPPER_SNAKE` | `ADCP_OPEN_METEO_MAX_ATTEMPTS` |
| Test | `test_<subject>_<expected_behaviour>` | `test_upsert_is_idempotent` |
| Branch | `feat/m4-idempotent-upsert` | |
| Commit | Conventional Commits | `feat(db): add idempotent observation upsert` |

---

## 17. Milestones M1 onward

Each milestone ends in a state a reviewer can run. "Exit criteria" are binary - a
milestone is not done because the code exists, but because the criteria pass.

### M1 - Scaffold and design (complete)

**Deliverables**

- `pyproject.toml` with runtime + dev dependency groups, script entry point, and
  ruff/mypy/pytest/coverage configuration.
- `src/adcp` package: `__init__`, `__main__`, `cli`, `config`, `logging`,
  `exit_codes`, `py.typed`.
- `tests/` with scaffold-only smoke tests.
- `docs/PLAN.md` (this document), `README.md` skeleton, `.env.example`,
  `.gitignore`, `.dockerignore`, `Dockerfile`, `docker-compose.yml`, CI workflow,
  `LICENSE`.

**Exit criteria**

| # | Criterion | Evidence |
| --- | --- | --- |
| 1 | `adcp --version`, `adcp config show`, `adcp config check` work | CLI run output |
| 2 | `pytest` passes, `compileall` clean | Command output |
| 3 | `docker compose config -q` exits 0 | Command output |
| 4 | `adcp config check --json` masks the database password | Test assertion |
| 5 | Plan covers all 18 required sections | Document review |

**Deliberately absent:** API calls, ORM models, migrations, scheduling, business
logic. M1 must contain zero logic that M3 would have to rewrite.

### M2 - Database foundation (complete)

**Deliverables**

- SQLAlchemy 2.0 engine/pool factory from `Settings`; `psycopg` (v3) driver.
- Alembic configured with the `locations`, `weather_hourly`, `ingestion_runs`,
  `ingestion_run_errors`, `ingestion_watermarks` tables from §5, plus the
  `ingestion_status` enum.
- Repository skeletons with typed signatures (no ingestion logic yet).
- `adcp db ping`, `adcp db upgrade`, `adcp db current` commands.
- Advisory-lock helper (`db/lock.py`) with tests proving mutual exclusion.
- `tests/conftest.py` PostgreSQL fixture (testcontainers with a Compose fallback).
- A committed lockfile (`uv lock` or `pip-compile`) so CI and production install the
  exact dependency set that was tested.

**Exit criteria**

| # | Criterion | Evidence |
| --- | --- | --- |
| 1 | `upgrade head` from an empty database creates the full schema; `downgrade -1`, `downgrade base`, and a full re-upgrade round-trip cleanly | `tests/integration/test_migrations.py`; CLI output recorded in the M2 report |
| 2 | Constraints reject a duplicate natural key, an out-of-range latitude, a non-hour-aligned timestamp, an unknown `source`, a bad slug, a backwards `finished_at`, and a missing parent row | `tests/integration/test_schema_constraints.py` |
| 3 | Core table definitions match the migrated database (`alembic check` clean) | `test_core_tables_match_the_migrated_schema`, `python -m alembic check` |
| 4 | `adcp db ping` exits 0 against the Compose database and 1 (no traceback) when unreachable | `tests/integration/test_db_cli.py`; CLI output recorded in the M2 report |
| 5 | Two connections cannot hold the same advisory lock; the lock is released on context exit and on exception, and survives a ROLLBACK | `tests/integration/test_advisory_lock.py` |
| 6 | No credential reaches stdout, stderr, or a log line | `test_ping_against_an_unreachable_database_is_a_clean_failure`, `test_db_ping_json_is_machine_readable_and_masked` |

**Decisions taken during M2**

- **ADR-006 (synchronous I/O with a bounded thread pool)** supersedes the M1 asyncio
  draft; see section 3.5.
- **Core tables, not ORM entities**, exactly as section 3.3 proposed:
  ``adcp/db/tables.py`` holds the schema in code and Alembic's ``env.py`` points
  ``target_metadata`` at it.
- **Advisory locks use the two-key form** (`pg_try_advisory_lock(namespace, hashtext(key))`)
  so ADCP cannot collide with single-key locks taken by other applications in the
  same database (section 7.4).
- **Integration fixtures live in ``tests/integration/conftest.py``** rather than the
  root conftest, so unit tests never import Testcontainers.

**Deliberately deferred to M4:** the weather upsert, run finishing/counters, and
watermark advancement remain typed stubs that raise ``NotImplementedError`` naming
their milestone, because each one is inseparable from the idempotency and
transaction design.

### M3a - API/source layer (complete)

**Deliverables**

- `models/` domain types (`Location`, `ObservationSource`, `WeatherObservation`,
  `WeatherSeries`, window specs) and `ports.py::WeatherSource`.
- `api/schemas.py`, `api/requests.py`, `api/mapping.py`, `api/open_meteo.py`: strict
  wire models, deterministic request construction, structured anomaly handling, and
  the HTTP adapter (configurable endpoint, per-phase timeouts, whole-request budget,
  streaming size cap, redirect cap, API-key injection).
- `resilience.py`: `RetryableUpstreamError`-based classification, full-jitter
  exponential backoff, `Retry-After` support, and an injectable sleep/clock.
- Recorded + derived contract fixtures and their recorder script.
- Unit suite with a mocked transport; an opt-in live smoke test that never runs by
  default.

**Exit criteria**

| # | Criterion | Evidence |
| --- | --- | --- |
| 1 | A recorded live payload parses into domain types with `Decimal` measurements, UTC timestamps, and grid metadata | `test_successful_fetch_returns_a_domain_series`, `tests/test_api_schemas.py` |
| 2 | Retry tests cover 429 + `Retry-After`, 503-then-200, connect/read timeouts, max attempts, and budget exhaustion; the whole API suite runs in well under a second because sleeps are injected | `tests/test_resilience.py`, `tests/test_api_open_meteo.py` |
| 3 | Permanent failures (4xx, malformed JSON, schema violations, oversized bodies, provider error documents) are never retried | same file, one test per class |
| 4 | Every anomaly in section 9.2/9.4 produces a structured `SchemaError` with field paths | `test_payload_anomalies_become_structured_schema_errors` |
| 5 | Requests are built from configured locations, not constants | `tests/integration/test_api_with_database_locations.py` |
| 6 | The API layer never imports the database layer, and HTTP is confined to the adapter and its policy | `tests/test_architecture.py` |
| 7 | Structured logs carry request context and no credentials | `test_success_is_logged_with_structured_fields`, `test_api_key_is_sent_but_never_logged` |

**Deliberately deferred to M3b/M4:** the validation layer (section 9.3 row rules,
rejection budgets), `row_hash`, `adcp collect --dry-run`, and the `locations` CLI
commands. M3a stops at the boundary those layers consume.

### M3b - Validation layer and dry-run collection (complete)

**Deliverables**

- `validation/rules.py` + `validation/validator.py`: all section 9.3 row rules
  producing accepted rows and reasoned rejections.
- `row_hash` with decimal normalisation and a `hash_version`.
- `adcp collect --location <slug> --dry-run`: fetch, validate, report - no writes.
- `scripts/seed_locations.py` for demo data (the CLI `locations` commands moved to
  M5, where backfill needs them for range selection).

**Exit criteria**

| # | Criterion | Evidence |
| --- | --- | --- |
| 1 | `--dry-run` writes nothing at all: no rows, no run row, no watermark | `test_dry_run_writes_nothing` |
| 2 | Every rule in section 9.3 has a positive and a negative case | `tests/test_validation_rules.py`, `tests/test_validation_validator.py` |
| 3 | `row_hash` is deterministic, collapses equivalent records, and changes only for meaningful data changes | `tests/test_normalization.py`, `test_unquantised_values_hash_consistently` |

### M4 - Idempotent persistence, run tracking, and incremental ingestion (complete)

**Deliverables**

- `db/repository.py`: batched `INSERT ... ON CONFLICT DO UPDATE ... WHERE row_hash IS
  DISTINCT FROM` with `RETURNING (xmax = 0)` counters, one transaction per location.
- `db/run_tracker.py`: run lifecycle, counter updates, error recording with
  credential masking and bounded payload samples.
- `db/watermark_store.py`: per-location/source watermark read and monotonic
  transactional advance.
- `pipeline/window.py` (watermark -> storage window + accept range, overlap,
  truncation) and `pipeline/service.py` (bounded concurrency, failure budget,
  rejection accounting, partial-status computation).
- Real `adcp collect` with exit codes 0/1/2/3, `--json`, `--dry-run`, and
  `--location`/`--lookback-hours`/`--overlap-hours` overrides.
- `adcp runs list|show|errors` and `adcp db stats` are **deferred to M5**: the
  accounting they would read is already persisted and asserted by tests.

**Exit criteria**

| # | Criterion | Evidence |
| --- | --- | --- |
| 1 | The idempotency matrix of section 10.6 passes against real PostgreSQL | `tests/integration/test_weather_repository.py`, `test_pipeline_service.py` |
| 2 | Unchanged rows are a true storage no-op; changed rows update in place with `revision_count` | `test_identical_rerun_is_a_true_storage_no_op`, `test_changed_value_updates_the_row_in_place` |
| 3 | Forecast and archive coexist for the same hour | `test_forecast_and_archive_rows_coexist_for_the_same_hour` |
| 4 | A failing location never rolls back another, and the failure budget ends a bad run | `test_one_failing_location_does_not_roll_back_another`, `test_failure_budget_stops_the_run` |
| 5 | Watermarks advance only with the rows they describe; a crash before commit re-collects the same window | `tests/integration/test_watermark_store.py`, `test_write_failure_rolls_back_rows_and_watermark`, `test_crash_before_commit_leaves_the_window_collectable` |
| 6 | Rejections are recorded with reason, location, timestamp, and source; the reject budget withholds a bad payload | `test_domain_invalid_rows_are_rejected_and_recorded`, `test_reject_budget_failure_writes_nothing` |
| 7 | `adcp collect` exits 0/1/2/3 as documented and never leaks credentials | `tests/test_collect_cli.py`, `tests/integration/test_collect_cli_db.py` |

### M5a - Scheduling and operations (complete)

**Deliverables**

- `scheduler.py`: `SchedulerPlan`/`build_schedule` (hourly cron or the
  configuration-driven development interval) and `CollectionScheduler` (job
  registration, in-process overlap guard, signal handling, graceful shutdown,
  structured operational logging).
- `adcp schedule`: startup preflight (config, timezone, minute, database
  reachability, schema at head), the long-running loop, and documented exit codes.
- `scripts/scheduler_demo.py`: a controlled development run that proves cycles,
  idempotency, and clean shutdown.
- The Compose `app` profile now runs `adcp schedule` with a 90 s stop grace period.

**Exit criteria**

| # | Criterion | Evidence |
| --- | --- | --- |
| 1 | The schedule is built from configuration (hourly model and development interval) and invalid values are rejected | `tests/test_scheduler.py`, `tests/test_schedule_cli.py` |
| 2 | Scheduled runs invoke the *same* collection path as `adcp collect` | `run_collection_once` is the job body; `tests/integration/test_schedule_cli_db.py` asserts scheduled runs are labelled `trigger=scheduler` |
| 3 | Overlapping ticks and lock contention cannot produce conflicting runs | `test_overlapping_ticks_are_skipped`, `test_lock_contention_is_reported_as_a_skipped_run`, plus the advisory-lock tests from M2 |
| 4 | One failed cycle never stops the scheduler | `test_a_failing_collection_is_logged_and_does_not_stop_the_scheduler`, `test_scheduler_survives_a_failed_cycle_then_collects` |
| 5 | Shutdown is graceful and completes | `test_shutdown_waits_for_the_running_collection`, `test_signal_handler_requests_a_clean_shutdown`, and the real cycle tests that shut the scheduler down mid-loop |
| 6 | Repeated cycles write no duplicates | `test_repeated_cycles_do_not_duplicate_rows` |

### M5b - Backfill, health, and run inspection (not implemented)

None of the commands below were built. The design is kept here because the
supporting pieces exist and the work is well understood; `docs/queries.sql`
covers the read-only parts (freshness, gaps, run history, failure digest) and
`adcp collect --lookback-hours N` covers gap repair today.

**Deliverables**

- `adcp backfill --from --to [--chunk-days]` with source routing (archive vs
  historical forecast), plus the `DateRange` window planner it needs.
- `adcp runs list|show|errors` and `adcp db stats` (carried over from M4: the
  accounting they read is already persisted and covered by tests).
- `adcp locations list|add|disable|import` (needed by backfill for range selection).
- `adcp health` with documented thresholds and a non-zero exit code on breach.
- `adcp db gaps --location --from --to`.
- `docs/RUNBOOK.md` with the §11.5 playbook.

**Exit criteria**

- Two concurrent `adcp schedule`/`collect` processes: exactly one performs work; the
  other logs `ingest.run.skipped` and exits 0.
- A 3-month backfill completes with monthly chunking and correct `source` routing;
  running it twice adds zero rows.
- `adcp health` exits non-zero when the newest observation is older than the
  configured threshold (verified with a seeded stale row).
- Compose `docker compose --profile app up` runs `postgres` + a scheduled collector
  unattended for one hour without manual intervention.

### M6 - Observability, retention, and hardening (complete)

**Deliverables**

- Log redaction tests, `db.query.slow` instrumentation, per-run log correlation.
- Stale-run reaper (runs `running` for more than 2 × `ADCP_RUN_TIMEOUT_S` become
  `failed`).
- `adcp db prune` with dry-run default and a documented retention policy.
- Optional Prometheus exporter behind a flag/extra.
- Load/sanity test: 50 locations, 1-hour cadence, documented resource usage.
- Failure-mode tests for F1-F17 from §11.2 (as many as are reproducible in CI).

**Exit criteria**

| # | Criterion | Evidence |
| --- | --- | --- |
| 1 | Every row in the §11.2 catalogue has a test or an explicit justification | section 11.6 |
| 2 | Redaction holds for the whole event, including a traceback | `tests/test_logging.py::test_credentials_inside_a_traceback_are_redacted`, `tests/test_cli_errors.py` |
| 3 | Every log line of a run carries its `run_id` | `tests/integration/test_db_observability.py` |
| 4 | Slow statements are reported without their parameters | `test_slow_statements_are_logged_without_parameters` |
| 5 | The reaper converts a stale `running` row to `failed`, and a collection does it automatically | `tests/integration/test_run_maintenance.py` |
| 6 | `adcp db prune` reports by default and deletes only with `--apply`, never destroying provenance | same file |
| 7 | The run deadline stops new work while committed locations survive | `test_the_run_stops_at_its_wall_clock_budget` |
| 8 | A crashed run before the watermark commit is reprocessed safely | scenario Run 6 |
| 9 | 50 locations collect inside an hour at concurrency 4 | `scripts/load_check.py`, numbers recorded in the M6 report |

**Deliberately not implemented:** the optional Prometheus exporter (§12.7). The run
tables, `db.query.slow`, and the structured events already answer the operational
questions, and a metrics endpoint would add a dependency without a consumer.

### M7 - Portfolio polish and release (complete)

**Delivered**

- `README.md` rewritten as the portfolio entry point: problem, business scenario,
  capabilities table, Mermaid architecture and data-flow diagrams, technology
  stack, repository structure, quickstart, database setup, migration and CLI
  references, scheduling, captured sample output, reliability and data-quality
  sections, the idempotency explanation, testing, Docker, example SQL,
  limitations, and the independence disclaimer.
- `docs/ARCHITECTURE.md` - component, layering, per-run flow, one-location
  sequence, ER model, retry decision tree, run state machine, and operational
  workflow diagrams.
- `docs/queries.sql` - eight runnable queries (freshness, coverage, run history,
  daily outcomes, revision churn, forecast/archive coexistence, gap finder,
  failure digest).
- `docs/DEMO.md` - the narrated 3-5 minute demonstration.
- `docs/RUNBOOK.md` - triage, recovery, retention, and escalation procedures.
- `docs/CASE_STUDY.md` and `docs/PORTFOLIO.md` - the evidence-based case study and
  the client-facing explanation.
- `scripts/capture_samples.py` and `docs/samples/` - deterministic, offline,
  re-verifiable captured output (`--check` fails when the docs go stale).
- CI extended with a documentation-sample freshness check and a container job
  that builds the image and runs the installed console script as the non-root
  user, so neither the docs nor the packaging can drift from the code.

**Exit criteria**

| # | Criterion | Evidence |
| --- | --- | --- |
| 1 | A reviewer reaches a successful `adcp collect` in under five minutes from the README alone | `README.md` quickstart, `scripts/demo.py` |
| 2 | The demo runs end to end with only Docker + Python | `DEMO RESULT: PASS` |
| 3 | Capability links point at real code, not intentions | README "Key capabilities" table |
| 4 | Documented sample output is reproducible | `python scripts/capture_samples.py --check` |
| 5 | No unsupported claims | README "Limitations and assumptions"; every number in `CASE_STUDY.md` is traceable to a command |

**Not delivered from the original M7 scope:** the ADR set under `docs/adr/`, the
`demo.ps1`/`demo.sh` wrappers (the cross-platform `scripts/demo.py` covers both),
screenshots/GIFs, a `CHANGELOG`, and the `v1.0.0` tag. A Git tag and the badge
only become meaningful once the repository is published, so they are left to the
release step.

### Remaining work (next milestone candidate)

Ordered by value, not effort:

1. **`adcp backfill --from/--to`** with archive-vs-historical-forecast routing and
   month chunking - the largest functional gap (sections 4.4 and 17/M5b).
2. **`adcp runs list|show|errors`** - the read-only run forensics the tables
   already support.
3. **`adcp health`** - a freshness threshold with a non-zero exit code, using
   `ADCP_HEALTHCHECK_FRESHNESS_HOURS` (already defined and validated).
4. **`adcp locations list|add|disable|import`** - the CLI equivalent of
   `scripts/seed_locations.py` plus a `locations.yaml` import.
5. **`adcp db gaps --location --from --to`** - promote query 7 into the CLI.
6. **`adcp db stats`** - row counts and table sizes.
7. `docs/adr/` for the five decisions in section 3.4, if the repository is used
   as a teaching artefact.

---

## 18. Portfolio/demo requirements

### 18.1 What a reviewer should be able to do in five minutes

```bash
git clone <repo> && cd adcp
python -m venv .venv && .venv\Scripts\activate
pip install -e . --group dev
copy .env.example .env
docker compose up -d postgres
adcp db upgrade
python scripts/seed_locations.py
adcp collect --location belgrade-rs            # real data, real timestamps
adcp collect --location belgrade-rs            # second run: 0 inserted, 0 updated
# run history and errors (the `runs` command is not built; use docs/queries.sql)
docker exec -i adcp-postgres psql -U adcp -d adcp < docs/queries.sql
```

The second `collect` printing zero changes is the single most important demo moment:
it proves idempotency without the reviewer having to read the code.

### 18.2 Required artefacts

| Artefact | Purpose |
| --- | --- |
| README hero paragraph + architecture diagram | Explain the project in 30 seconds |
| "What this demonstrates" table linking to real code | Map capabilities to files |
| Quickstart with copy-pasteable commands | Remove all setup friction |
| `docs/PLAN.md` (this document) | Show design maturity and trade-off reasoning |
| `docs/ARCHITECTURE.md` | Show decisions were deliberate (ADRs not yet written) |
| `docs/RUNBOOK.md` | Show operational thinking |
| `docs/queries.sql` | Show the data is actually useful |
| `docs/DEMO.md` + `scripts/demo.py` | Reproducible end-to-end demonstration |
| `docs/samples/` (regenerated by `scripts/capture_samples.py`) | Evidence of working behaviour that cannot silently go stale |
| `docs/CASE_STUDY.md`, `docs/PORTFOLIO.md` | Measured results and the client-facing narrative |

### 18.3 Demo data and queries

- Ship `scripts/seed_locations.py` with three well-known cities (Belgrade,
  Reykjavik, Ushuaia) chosen to show a range of climates in the data. A
  `locations.yaml` import is designed (section 13.2) but not built.
- `docs/queries.sql` contains nine read-only queries: freshness per
  location/source, coverage and revision summary, run history, run outcome by
  day, forecast revision churn, hours where two sources coexist (the payoff for
  keeping `source` in the natural key), a missing-hour gap finder, a failure
  digest, and the hottest/coldest stored hour per location.

### 18.4 Narrative and honesty requirements

- Document what the project deliberately does *not* do (§2.2) and why.
- Document the free-tier constraints and Open-Meteo attribution prominently.
- Include a "limitations and next steps" section: partitioning at scale, secrets
  management, the optional revision-history table, metrics exporter, multi-region
  deploys.
- No committed secrets, no fabricated screenshots, no overstated claims in the README
  ("production-style", not "production-ready for any workload").

### 18.5 Presentation details

- Commit history tells the story: one conventional commit per milestone slice, with
  the PLAN as the reference.
- README badges: CI status, Python versions, PostgreSQL, and licence. A coverage
  badge is deliberately omitted until a hosted coverage service is configured,
  because an unbacked badge would be a claim rather than a measurement.
- Repository description and topics set on the hosting platform
  (`data-engineering`, `etl`, `postgresql`, `python`, `open-meteo`,
  `idempotency`, `observability`).
- The five-minute walkthrough rehearsed aloud: problem -> architecture -> live
  idempotency demo -> run-tracking query -> what's next.

---

## Appendix A - Assumptions to verify during implementation

These are stated explicitly so that a wrong assumption is caught by a test rather
than discovered in production.

| # | Assumption | Verified in |
| --- | --- | --- |
| A1 | Open-Meteo returns `time` values without an offset when `timezone=UTC`, so the client must localise using the request timezone | M3 (contract test) |
| A2 | `hourly_units` keys mirror the requested variable names exactly | M3 |
| A3 | Comma-separated coordinates return a JSON array in the same order as requested | M5 |
| A4 | The archive endpoint rejects (or returns nulls for) the most recent ~5 days | M5 (backfill source routing) |
| A5 | Current free-tier rate limits (requests/day, requests/minute) and the exact 429 body shape | M3, encoded as documentation + settings |
| A6 | `past_days` maximum still permits a 72-hour lookback plus overlap in a single request | M4 |
| A7 | `xmax = 0` reliably distinguishes inserts from updates in the `RETURNING` clause on PostgreSQL 17 | M4 (integration test) |
| A8 | `pg_try_advisory_lock` is released automatically when the session ends abruptly | M2 (integration test) |

## Appendix B - Glossary

| Term | Meaning |
| --- | --- |
| Watermark | The newest `observed_at` successfully committed for a (location, source) pair |
| Overlap | Hours re-fetched before the watermark to absorb upstream forecast revisions |
| Natural key | `(location_id, observed_at, source)` - what makes a row unique in reality |
| Row hash | sha256 of normalised measurement values; detects real change vs re-observation |
| Revision | An upsert that changes a stored measurement's value |
| Partial run | A run where some locations or rows failed but useful data was committed |
| Source | Which upstream product produced the observation (`forecast`, `historical_forecast`, `archive`) |
| Advisory lock | PostgreSQL session-level lock used to guarantee a single concurrent run |
| Reject | A row that failed domain validation and was not written |
| Quarantine | Storing the offending payload sample in `ingestion_run_errors` for forensics |
