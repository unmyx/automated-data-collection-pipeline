# ADCP - Automated Data Collection Pipeline

[![CI](https://github.com/depduris/adcp/actions/workflows/ci.yml/badge.svg)](https://github.com/depduris/adcp/actions/workflows/ci.yml)
[![Python 3.12+](https://img.shields.io/badge/python-3.12%2B-blue.svg)](https://www.python.org/downloads/)
[![PostgreSQL 17](https://img.shields.io/badge/postgresql-17-336791.svg)](https://www.postgresql.org/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

A production-style Python service that collects hourly weather observations from
the public [Open-Meteo](https://open-meteo.com/) REST API and stores them in
PostgreSQL - **idempotently, incrementally, and with a complete run audit.**

> **This is an independent portfolio project.** It is not client work, has not
> been deployed to production, and uses public data with synthetic demo
> locations. No uptime, revenue, or business-impact figure is claimed anywhere
> in this repository. See [Limitations and assumptions](#limitations-and-assumptions).

**Jump to:** [Problem](#problem) · [Capabilities](#key-capabilities) ·
[Architecture](#architecture) · [Quickstart](#quickstart) ·
[CLI reference](#cli-reference) · [Idempotency](#how-idempotency-works) ·
[Testing](#testing) · [Portfolio docs](#documentation)

---

## Problem

Teams that need an hourly history from a third-party API usually start with a
script that calls the endpoint and inserts rows. That script fails in predictable
ways:

- it runs late, gets killed, or overlaps with itself, and writes duplicate rows;
- it treats upstream data as immutable, although forecast models are revised
  several times a day, so stored rows silently go stale;
- it fails quietly, so nobody notices that a location has had no data for days;
- it leaves no audit trail, so "did the job run, and which locations failed?" has
  no answer.

The result is a dataset that looks complete but contains gaps and duplicates, and
no operational trail that would prove otherwise.

## Business scenario

Picture an energy or agriculture analytics team that needs a reliable hourly
weather history for a fixed set of sites. They have no weather infrastructure of
their own, they are not allowed to hand-maintain a CSV, and the downstream models
are only as good as the freshness and correctness of the history behind them.

This repository is the pipeline that team would run: configure the sites once,
schedule the collector, and then answer operational questions - *what ran, what
changed, what failed, and what is missing?* - from SQL rather than from memory.

## Key capabilities

| Capability | Where it lives |
| --- | --- |
| REST/JSON ingestion from a public API | [`src/adcp/api/open_meteo.py`](src/adcp/api/open_meteo.py) |
| Retries, per-phase timeouts, jittered backoff, rate-limit handling | [`src/adcp/resilience.py`](src/adcp/resilience.py) |
| Strict typed response models and anomaly handling | [`src/adcp/api/schemas.py`](src/adcp/api/schemas.py), [`src/adcp/api/mapping.py`](src/adcp/api/mapping.py) |
| Four validation layers with machine-readable rejections | [`src/adcp/validation`](src/adcp/validation) |
| PostgreSQL schema, constraints, indexes, migrations | [`src/adcp/db`](src/adcp/db), [`docs/PLAN.md`](docs/PLAN.md#5-postgresql-schema) |
| Idempotent upsert + content hashing | [`src/adcp/db/repository.py`](src/adcp/db/repository.py), [`src/adcp/normalization.py`](src/adcp/normalization.py) |
| Incremental ingestion via watermarks and an overlap window | [`src/adcp/pipeline/window.py`](src/adcp/pipeline/window.py) |
| Per-location transactions and partial-failure handling | [`src/adcp/pipeline/service.py`](src/adcp/pipeline/service.py) |
| Advisory-lock single-run guarantee | [`src/adcp/db/lock.py`](src/adcp/db/lock.py) |
| Ingestion/run tracking and error forensics | [`src/adcp/db/run_tracker.py`](src/adcp/db/run_tracker.py) |
| Structured JSON logging with recursive secret redaction | [`src/adcp/logging.py`](src/adcp/logging.py) |
| Scheduling with overlap safety and graceful shutdown | [`src/adcp/scheduler.py`](src/adcp/scheduler.py) |
| Automated tests: unit, mocked-HTTP, and real-PostgreSQL integration | [`tests/`](tests) |

## Architecture

Ports-and-adapters: the ingestion core depends on protocols, not on HTTP or SQL,
and the API layer never imports the database layer (enforced by
[`tests/test_architecture.py`](tests/test_architecture.py)).

```mermaid
flowchart LR
    cron["cron / timer / CI"] --> cli["adcp CLI<br/>collect · schedule · db · config"]
    human["developer"] --> cli
    cli --> scheduler["scheduler.py"]
    cli --> service["pipeline/service.py"]
    scheduler -->|"same job body"| service
    service --> api["api/open_meteo.py<br/>httpx"]
    service --> validation["validation/<br/>4 layers"]
    service --> db["db/<br/>repositories + lock"]
    api --> openmeteo["Open-Meteo REST API"]
    db --> postgres[("PostgreSQL 17")]
```

Full diagrams - component, per-run flow, one-location sequence, ER model, retry
decision tree, and the run state machine - are in
[`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

### Data flow for one run

```mermaid
flowchart TD
    a["validate configuration"] --> b{"take advisory lock"}
    b -- "held by another run" --> skip["log skip, exit 0"]
    b -- acquired --> c["reap stale runs, load active locations + watermarks"]
    c --> d["plan storage window per location"]
    d --> e["fetch Open-Meteo: timeouts + retries"]
    e --> f["parse -> validate -> normalise -> hash"]
    f --> g["upsert + advance watermark<br/>(one transaction per location)"]
    g --> h["record run counters and errors"]
    h --> i["compute status and exit code"]
```

## Technology stack

| Area | Choice |
| --- | --- |
| Language | Python 3.12+ (developed on 3.14), fully annotated, ships `py.typed` |
| API client | `httpx` |
| Retries | `tenacity` with a custom full-jitter backoff and request budget |
| Validation | `pydantic` v2 strict models + explicit domain rules |
| Database | PostgreSQL 17 |
| DB access | SQLAlchemy 2.0 Core + `psycopg` 3 |
| Migrations | Alembic (5 revisions) |
| Scheduling | APScheduler behind a thin wrapper |
| Logging | `structlog` (JSON and console renderers, recursive redaction) |
| CLI | `typer` |
| Configuration | `pydantic-settings` |
| Tests | `pytest`, `respx`, `testcontainers` |
| Quality | `ruff` (lint + format), `mypy --strict`, `compileall` |
| Packaging | `hatchling` + `uv.lock` |
| Runtime | Multi-stage Docker image, non-root, healthcheck |

## Repository structure

```
.
|-- src/adcp/                     # the package
|   |-- api/                      # Open-Meteo adapter: requests, schemas, mapping
|   |-- cli/                      # typer app: collect, schedule, db, config
|   |-- db/                       # engine, lock, repositories, tracking, migrations/
|   |-- models/                   # domain types (location, observation, run, window)
|   |-- pipeline/                 # CollectionService + window planning
|   |-- validation/               # rules + validator (4 layers)
|   |-- config.py                 # Settings (pydantic-settings)
|   |-- logging.py                # structlog configuration + redaction
|   |-- resilience.py             # timeout/retry/backoff policy
|   |-- scheduler.py              # APScheduler wrapper
|   `-- exit_codes.py             # the 0/1/2/3 process contract
|-- tests/                        # unit, mocked-HTTP, and PostgreSQL integration
|   |-- fixtures/open_meteo/      # recorded + derived API payloads
|   `-- integration/              # real SQL behaviour (skipped without PostgreSQL)
|-- docs/                         # PLAN, ARCHITECTURE, RUNBOOK, DEMO, PORTFOLIO, ...
|   `-- samples/                  # deterministic captured command output
|-- scripts/                      # demo, sample capture, load check, fixture recorder
|-- docker-compose.yml            # postgres + optional Adminer and app profiles
|-- Dockerfile                    # multi-stage, non-root, healthcheck
`-- pyproject.toml                # metadata, deps, ruff/mypy/pytest/coverage config
```

## Quickstart

Requirements: Python 3.12+, and Docker with the Compose plugin for the local
PostgreSQL instance.

```bash
git clone https://github.com/depduris/adcp.git
cd adcp

python -m venv .venv
.venv\Scripts\activate            # Windows
# source .venv/bin/activate       # macOS / Linux

pip install -e . --group dev
cp .env.example .env              # Windows: copy .env.example .env

docker compose up -d --wait postgres
adcp config check
adcp db upgrade
adcp db ping

python scripts/seed_locations.py  # three demo locations
adcp collect                      # real data from the public API
adcp collect                      # second run: inserts nothing
```

Prefer one command? `python scripts/demo.py` runs the whole sequence and prints
`DEMO RESULT: PASS`.

> On managed Windows machines that block freshly created `.exe` shims, use
> `python -m adcp <command>` instead of `adcp <command>`. Both are the same entry
> point (see [Troubleshooting](#troubleshooting)).

## Database setup

```bash
docker compose up -d --wait postgres      # PostgreSQL 17 + healthcheck
docker compose --profile tools up -d      # optional Adminer UI on :8080
docker compose config -q                  # validate the compose file
```

The container always listens on 5432, but it is **published on host port 55432**
so it never collides with a PostgreSQL you already run locally:

```
postgresql+psycopg://adcp:adcp_local_dev@localhost:55432/adcp
```

Those credentials are throwaway local-development defaults. Set `POSTGRES_PORT`
(and the matching port in `ADCP_DATABASE_URL`) if 55432 is taken; set
`ADCP_DATABASE_URL` to a real DSN anywhere else.

### Migrations

The schema is owned by Alembic - five revisions create `locations`,
`ingestion_runs`, `weather_hourly`, `ingestion_run_errors`, and
`ingestion_watermarks` ([schema reference](docs/PLAN.md#5-postgresql-schema)).

```bash
adcp db upgrade                            # -> head, reporting what was applied
adcp db current                            # applied revision vs head
adcp db current --check                    # exit 1 when behind (CI-friendly)
adcp db upgrade --revision 0003_weather_hourly
adcp db upgrade --json                     # one JSON document; .applied lists the revisions

# the upstream tool works too, and `check` detects code/database drift
python -m alembic upgrade head
python -m alembic current
python -m alembic check
python -m alembic downgrade -1
```

`alembic.ini` holds no credentials: both paths resolve the DSN from
`ADCP_DATABASE_URL` (`DATABASE_URL` is accepted as an alias).

### Retention

```bash
adcp db prune            # report only - the default
adcp db prune --apply    # delete what the report listed
```

Runs older than 365 days and error rows older than 90 days are eligible. Runs
that observations still reference are kept and counted separately, because
`weather_hourly` records which run first and last saw each row. The fact table is
never pruned.

## CLI reference

```bash
adcp --version                                   # same as `adcp version`

adcp config show [--json]                        # resolved settings, secrets masked
adcp config check                                # validate, exit 2 when unusable

adcp db ping [--json]                            # connectivity + server version
adcp db upgrade [--revision R] [--json]          # apply pending migrations
adcp db current [--check] [--json]               # schema revision
adcp db prune [--apply] [--json]                 # retention report / delete

adcp collect                                     # one collection run
adcp collect --location belgrade-rs              # repeatable; restrict to slugs
adcp collect --lookback-hours 168                # widen the window for one run
adcp collect --overlap-hours 6                   # re-read more recent history
adcp collect --dry-run                           # fetch + validate, write nothing
adcp collect --json                              # machine-readable summary

adcp schedule                                    # hourly at :07 UTC, until interrupted
adcp schedule --minute 15 --timezone Europe/Belgrade
adcp schedule --interval-seconds 15 --run-once   # development/demo cadence
```

Every command that reports data accepts `--json`, which writes exactly one JSON
document to stdout so it is safe to pipe. Database passwords are rendered as
`***` on stdout, on stderr, and in logs.

### Exit codes

| Code | Meaning |
| --- | --- |
| `0` | full success, or nothing to do (another run holds the lock, no active locations) |
| `1` | operational failure: the run failed, or the database is unreachable |
| `2` | invalid configuration or usage: bad flag combination, unknown location |
| `3` | partial success: some rows were written, something failed or was rejected |
| `130` | interrupted by the operator (Ctrl+C) |

Unexpected errors never dump a traceback at the user: the full stack goes to the
log (`cli.unexpected_error`) and the CLI prints one line with exit code 1.

## Sample collection output

Real output, captured deterministically by
[`scripts/capture_samples.py`](scripts/capture_samples.py) - see
[`docs/samples/`](docs/samples) for the full set:

```
$ adcp collect
Collection complete: succeeded
succeeded belgrade-rs              received=3 inserted=3 updated=0 unchanged=0 rejected=0 skipped=0
succeeded reykjavik-is             received=3 inserted=3 updated=0 unchanged=0 rejected=0 skipped=0
run_id        <run_id>
locations     2 ok, 0 failed, 2 requested
rows          received 6, inserted 6, updated 0, unchanged 0, rejected 0, skipped 0
requests      0 made, 0 retried
summary       (all good)
```

```json
$ adcp collect --json
{
  "locations_succeeded": 2,
  "rows_accepted": 6,
  "rows_inserted": 6,
  "rows_rejected": 0,
  "rows_unchanged": 0,
  "run_id": "<run_id>",
  "status": "succeeded",
  "trigger": "cli"
}
```

The same window collected twice ([`idempotent-rerun.txt`](docs/samples/idempotent-rerun.txt)):

```
run 1 counters:  {"rows_received": 6, "rows_inserted": 6, "rows_unchanged": 0, ...}
run 2 counters:  {"rows_received": 6, "rows_inserted": 0, "rows_unchanged": 6, ...}
stored rows after run 2: 6
```

Retries are visible as structured events
([`retry-log.jsonl`](docs/samples/retry-log.jsonl)): a `503`, a backoff, and a
successful second attempt:

```json
{"attempt": 1, "delay_s": 0.119, "error_type": "UpstreamServerError", "event": "api.request.retry", "http_status": 503, "level": "warning", "max_attempts": 3}
```

### Scheduling

`adcp schedule` decides *when* to run and delegates every collection to the same
code path as `adcp collect`, so scheduled and manual runs cannot drift apart. It
requires `ADCP_SCHEDULER_ENABLED=true` (the Compose `app` profile sets it).

- **Overlap is impossible.** The PostgreSQL advisory lock is the authority; on
  top of it the job runs with `max_instances=1`, and an overlapping tick is
  logged as `scheduler.collection.skipped`.
- **Failures are isolated.** A failed cycle is logged and recorded in
  PostgreSQL, and the schedule continues.
- **Shutdown is graceful.** SIGINT/SIGTERM (SIGBREAK on Windows) let the
  in-flight collection finish, stop the scheduler, and exit 0.
- **Everything is logged:** `scheduler.started`, `scheduler.next_run`,
  `scheduler.collection.triggered`, `scheduler.collection.completed`,
  `scheduler.collection.skipped`, `scheduler.collection.failed`,
  `scheduler.job.missed`, `scheduler.shutdown.requested`, `scheduler.stopped`.

```bash
python scripts/scheduler_demo.py --cycles 2 --interval-seconds 15
```

The demo runs the real command, waits for two collections, sends a graceful
shutdown, then checks PostgreSQL for duplicate hours and prints a PASS/FAIL
summary.

## How idempotency works

The natural key is `(location_id, observed_at, source)`, and every row carries a
content hash of its normalised values (`row_hash`). The write is:

```sql
INSERT INTO weather_hourly (...) VALUES (...)
ON CONFLICT (location_id, observed_at, source) DO UPDATE
   SET ...
 WHERE weather_hourly.row_hash IS DISTINCT FROM EXCLUDED.row_hash;
```

The `WHERE` clause is the important part: if the incoming data is identical, the
statement updates *nothing*, so `last_collected_at` and `revision_count` stay
untouched. A re-run of the same window is a true storage no-op, not a rewrite
with the same values.

```mermaid
flowchart LR
    nk["natural key<br/>(location, hour, source)"] --> exists{"row exists?"}
    exists -- no --> ins["INSERT"]
    exists -- yes --> hash{"row_hash differs?"}
    hash -- no --> noop["no-op<br/>(nothing written)"]
    hash -- yes --> upd["UPDATE values,<br/>revision_count += 1"]
```

| Scenario | Result |
| --- | --- |
| Same window collected twice | second run: `inserted 0, updated 0, unchanged N` |
| Provider revised a stored hour | that row updates in place and `revision_count` increments |
| Process killed before commit | nothing was written; the next run re-collects the same window |
| Two runs at once | the advisory lock admits one; the other exits 0 having done nothing |

Because the watermark advances in the same transaction as the rows it describes,
the cursor can never point past data that was rolled back - which is what makes
"just run it again" a safe recovery procedure.

## Reliability features

- **Single-run guarantee** - `pg_try_advisory_lock(namespace, hashtext(key))`;
  lock contention is logged and reported without a traceback.
- **Per-location transactions** - one failure rolls back that location only; a
  successful location stays committed and the run becomes `partial`.
- **Failure budget** - when more than `ADCP_INGEST_FAILURE_BUDGET_RATIO` of
  locations fail, the run stops early and is recorded as `failed`.
- **Run wall-clock budget** - `ADCP_RUN_TIMEOUT_S` stops new locations; work
  already committed survives.
- **Stale-run reaper** - a run left `running` for more than twice the budget is
  marked `failed` at the start of the next collection.
- **Retry taxonomy** - timeouts, connection failures, 5xx, and 429 (honouring
  `Retry-After`, capped) are retried with full-jitter exponential backoff; 4xx,
  malformed JSON, schema violations, and oversized bodies never are.
- **Timeouts everywhere** - connect/read/write per attempt, plus a whole-request
  budget across all attempts.
- **Structured logging** - every line of a run carries its `run_id`, and
  credentials are redacted recursively, including inside tracebacks.
- **Slow-query visibility** - statements over `ADCP_DB_SLOW_QUERY_MS` log
  `db.query.slow` with the statement text, never its parameters.
- **Production guards** - with `ADCP_ENV=prod`, the shipped local DSN and console
  log format are refused at startup with actionable messages.

## Data-quality handling

Payloads pass through four layers before anything is written:

1. **Transport** - HTTP status, body size, and JSON decoding.
2. **Schema** - strict typed models reject missing fields, wrong types, missing
   hourly sections, ragged arrays, non-finite numbers, changed units, and
   unparseable or out-of-order timestamps.
3. **Domain** - numeric ranges, allowed weather codes, hour alignment, and source
   identity.
4. **Referential** - location identity, coordinate drift against the configured
   site, and membership of the accept window.

Rows that fail a layer get a machine-readable rejection code and are recorded in
`ingestion_run_errors` with the run, location, phase, timestamp, and a bounded
credential-masked payload sample. Above the per-location reject budget
(`ADCP_INGEST_MAX_INVALID_ROW_RATIO`, default 25%) the location's transaction
rolls back entirely.

[`docs/samples/rejection-result.txt`](docs/samples/rejection-result.txt) shows
both sides of that threshold: 1 rejected row in a 5-hour window (20%) is a
`partial` run, while 1 in 3 (33%) fails the location and the run, with the reason
recorded.

## Testing

```bash
pytest -m "not integration"     # fast unit tests: no database, no network
pytest -m integration           # PostgreSQL-backed tests
pytest --cov                    # everything, with the coverage gate (85%)
```

Current state: **576 tests passing**, **95.8%** line and branch coverage on
`src/adcp`. The API layer is unit-tested against mocked HTTP (`respx`), so the
normal suite never touches the network. One opt-in test calls the real API:

```bash
ADCP_LIVE_API_TESTS=true pytest tests/test_live_open_meteo.py
```

Integration tests need a real PostgreSQL (never SQLite - upserts, `numeric`
semantics, and advisory locks are PostgreSQL-specific). The suite picks a target
in this order:

1. `ADCP_TEST_DATABASE_URL`, if set - used by CI and by anyone with their own server;
2. **Testcontainers** - a throwaway `postgres:17-alpine`, when Docker is available;
3. the Compose server, creating a sibling `adcp_test` database so the application
   database is untouched.

If none is reachable the integration tests **skip**, so `pytest` still passes on
a machine without Docker. Tests truncate tables between cases, so the target must
be disposable: a database name that does not contain `test` is refused unless
`ADCP_TEST_ALLOW_ANY_DATABASE=true` is set.

```bash
export ADCP_TEST_DATABASE_URL=postgresql+psycopg://adcp:adcp_local_dev@localhost:55432/adcp_test
```

Other checks used by CI:

```bash
ruff check .              # lint
ruff format --check .     # formatting
mypy                      # static types (strict)
python -m compileall -q src tests scripts
python scripts/capture_samples.py --check   # docs/samples are up to date
docker build -t adcp:ci .                   # the image builds, non-root, CLI runs
```

CI runs the unit suite with no services at all, and the quality job adds a
PostgreSQL 17 service so it can also apply the migrations from empty, detect
schema drift, verify the documentation samples, and run the full suite with the
coverage gate. A third job builds the container image and checks that the
installed console script runs as uid 10001.

## Docker

```bash
docker compose config -q                  # validate the compose file
docker compose up -d --wait postgres      # PostgreSQL only
docker compose --profile tools up -d      # + Adminer on http://localhost:8080
docker compose --profile app up --build   # + the collector, running `adcp schedule`
docker compose --profile app logs -f app
```

The image is multi-stage, runs as a non-root user (uid 10001), ships no secrets,
and has a healthcheck. The `app` service waits for PostgreSQL to be healthy and
has a 90-second stop grace period so a SIGTERM lets an in-flight collection
finish; being killed mid-run is safe regardless, because watermarks only move on
commit.

## Example SQL queries

[`docs/queries.sql`](docs/queries.sql) is a runnable pack (freshness, coverage,
run history, daily outcomes, revision churn, forecast/archive coexistence, a gap
finder, a failure digest, and temperature extremes per location):

```bash
docker exec -i adcp-postgres psql -U adcp -d adcp < docs/queries.sql
# PowerShell: Get-Content docs/queries.sql | docker exec -i adcp-postgres psql -U adcp -d adcp
```

A taste - what do we actually have, and is it fresh?

```sql
SELECT l.slug, w.source, count(*) AS rows,
       min(w.observed_at)::date AS first_day,
       max(w.observed_at)::date AS last_day,
       coalesce(sum(w.revision_count), 0) AS revisions
FROM weather_hourly w
JOIN locations l ON l.id = w.location_id
GROUP BY l.slug, w.source
ORDER BY l.slug, w.source;
```

```
 slug         | source   | rows | first_day  | last_day   | revisions
--------------+----------+------+------------+------------+-----------
 belgrade-rs  | forecast | 4    | 2026-10-01 | 2026-10-01 | 0
 reykjavik-is | forecast | 4    | 2026-10-01 | 2026-10-01 | 0
```

## Configuration

Configuration comes from environment variables, an optional `.env` file, and CLI
flags - in that order of increasing precedence. [`.env.example`](.env.example)
documents every setting with its default, and the full reference with validation
rules is in [`docs/PLAN.md`](docs/PLAN.md#15-configurationenvironment-variables).

The settings that matter most in normal operation:

| Variable | Default | Purpose |
| --- | --- | --- |
| `ADCP_DATABASE_URL` | local Compose DSN | PostgreSQL connection string |
| `ADCP_ENV` | `local` | `local`, `dev`, `staging`, or `prod`; `prod` enables the safety guards |
| `ADCP_LOG_FORMAT` | `console` | `json` for collectors, `console` for a human |
| `ADCP_LOG_LEVEL` | `INFO` | `DEBUG` through `CRITICAL` |
| `ADCP_INGEST_LOOKBACK_HOURS` | `72` | how far back a first or gap-recovery run may reach |
| `ADCP_INGEST_OVERLAP_HOURS` | `24` | how much recent history is re-read to catch revisions |
| `ADCP_INGEST_FAILURE_BUDGET_RATIO` | `0.5` | failed-location share that fails the run |
| `ADCP_INGEST_MAX_INVALID_ROW_RATIO` | `0.25` | rejected-row share that fails a location |
| `ADCP_RUN_TIMEOUT_S` | `3600` | whole-run wall-clock budget |
| `ADCP_SCHEDULER_ENABLED` | `false` | required for `adcp schedule` |
| `ADCP_SCHEDULER_MINUTE` | `7` | minute past the hour for the hourly job |
| `ADCP_SCHEDULER_TIMEZONE` | `UTC` | timezone the schedule is evaluated in |
| `ADCP_OPEN_METEO_MAX_ATTEMPTS` | `5` | attempts per request, including the first |
| `ADCP_OPEN_METEO_MAX_CONCURRENCY` | `4` | parallel location fetches |

```bash
python -m adcp config show --json     # resolved values, password masked
```

## Documentation

| Document | What it is |
| --- | --- |
| [`docs/PLAN.md`](docs/PLAN.md) | The full design: problem, scope, architecture, schema, API, scheduling, resilience, validation, idempotency, failures, observability, CLI, testing, configuration, milestones |
| [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) | Diagrams: components, data flow, sequence, ER model, retry tree, run states, operational workflow |
| [`docs/DEMO.md`](docs/DEMO.md) | A narrated 3-5 minute demonstration with the expected output at each step |
| [`docs/RUNBOOK.md`](docs/RUNBOOK.md) | Operator procedures: triage, recovery, retention, escalation |
| [`docs/CASE_STUDY.md`](docs/CASE_STUDY.md) | Problem, solution, technologies, measured results, and testing summary |
| [`docs/PORTFOLIO.md`](docs/PORTFOLIO.md) | The same project explained for a prospective client |
| [`docs/queries.sql`](docs/queries.sql) | Runnable SQL for freshness, coverage, run history, gaps, and failures |
| [`docs/samples/`](docs/samples) | Deterministic captured output used throughout the docs |

## Limitations and assumptions

Stated plainly, because a portfolio project is more useful when its edges are
visible:

- **Single source, single destination.** One public API and one PostgreSQL
  instance. There is no Kafka, no data lake, and no multi-tenant routing.
- **Forecast and historical-forecast ingestion only.** The archive endpoint is
  modelled (`source='archive'`, a `DateRange` window, `db/repository` support),
  but scheduled collection requests the forecast endpoint; the backfill command
  that would use the archive has not been built.
- **Latest-value semantics.** `weather_hourly` stores the newest view of each
  hour plus a revision counter. Full revision history (an append-only table) was
  deliberately deferred - the provenance columns leave room for it.
- **Docker-based scheduling.** `adcp schedule` is an in-process APScheduler loop;
  it is not a distributed scheduler. The advisory lock still makes running it
  twice safe.
- **No metrics endpoint.** Structured logs and the run tables answer the
  operational questions; a Prometheus exporter was deliberately not added.
- **Measured on one development machine.** The 50-location check
  (`scripts/load_check.py`) is a sanity check against a local PostgreSQL, not a
  capacity claim. Its outcome also depends on upstream health: a run that meets a
  provider 503 records a `partial` run (the other locations still commit) rather
  than retrying forever.
- **Free-tier etiquette.** The default concurrency, backoff, and chunk pause
  assume the Open-Meteo free tier. Attribution and licence terms are the
  operator's responsibility.

## Data source and licence

Weather data is provided by [Open-Meteo](https://open-meteo.com/) under
[CC BY 4.0](https://creativecommons.org/licenses/by/4.0/). The free API tier is
for non-commercial use and requires attribution; see the
[Open-Meteo terms](https://open-meteo.com/en/terms). This project is an
engineering portfolio piece and is not affiliated with Open-Meteo.

Code in this repository is released under the MIT licence (see [`LICENSE`](LICENSE)).

## Troubleshooting

- **`ImportError: DLL load failed ... blocked by an Application Control policy`**
  while importing SQLAlchemy: some managed Windows machines refuse to load
  unsigned native extensions from a fresh virtualenv. SQLAlchemy's C
  accelerators are optional - delete the blocked `sqlalchemy\**_cy.cp3*.pyd`
  files inside `.venv` and the pure-Python fallbacks shipped alongside them are
  used instead. Linux, CI, and the container image are unaffected.
- **`adcp: command not found`, or an `.exe` blocked by policy**: use
  `python -m adcp <command>`, which always works.
- **Port 55432 already in use**: set `POSTGRES_PORT` and the matching port in
  `ADCP_DATABASE_URL`.
- **`password authentication failed`**: the Compose volume kept credentials from
  an earlier run. `docker compose down` (add `-v` to discard the local data), then
  `docker compose up -d --wait postgres`.
