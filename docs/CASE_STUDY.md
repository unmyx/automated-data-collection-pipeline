# Case study: idempotent weather ingestion pipeline

A factual account of what this repository contains, what was measured, and what
it demonstrates. It is written to be reused as the source for a CV entry, an
Upwork portfolio item, or a short written answer to "show me a data engineering
project".

> **Independence disclaimer.** This is an independent, self-directed project. It
> is not client work, it has not been deployed to production, and it uses public
> data (Open-Meteo) and synthetic locations. No uptime, revenue, or
> business-impact figure is claimed anywhere in this repository.

---

## Problem

Teams that need an hourly weather history usually start with a script that calls
an API and inserts rows. That script fails in predictable ways:

- it runs late, gets killed, or overlaps with itself and writes duplicate rows;
- it treats upstream data as immutable, although forecast models are revised
  several times a day, so stored rows silently go stale;
- it fails quietly, so nobody notices that a location has had no data for days;
- it leaves no audit trail, so "did the job run, and which locations failed?" has
  no answer.

The result is a dataset that looks complete but contains gaps and duplicates, and
no operational trail that would prove otherwise.

## Solution

A production-style Python service that collects hourly weather observations for
a configured set of locations and stores them in PostgreSQL with **idempotent,
incremental, validated writes and a complete run audit**.

Concretely, each run:

1. takes a PostgreSQL advisory lock, so two runs can never collect at once;
2. loads active locations and their per-source watermarks from the database;
3. plans a storage window per location (watermark minus an overlap, capped by a
   configurable lookback);
4. fetches hourly variables from the public Open-Meteo REST API with bounded
   retries, per-phase timeouts, and jittered backoff;
5. parses the payload into strict typed models, then validates rows through four
   layers (transport, schema, domain, referential);
6. upserts each location's rows in its own transaction, keyed on
   `(location_id, observed_at, source)`, skipping updates when the content hash
   is unchanged;
7. advances the watermark inside that same transaction;
8. records the run, its counters, and any errors, and emits structured JSON logs.

## Architecture

Ports-and-adapters layering, with the ingestion core independent of both HTTP and
SQL. Diagrams are in [`ARCHITECTURE.md`](ARCHITECTURE.md).

| Layer | Responsibility | Code |
| --- | --- | --- |
| CLI / scheduler | parse flags, map results to exit codes, decide *when* | `cli/`, `scheduler.py` |
| Pipeline core | one run: lock, plan, fetch, validate, persist, account | `pipeline/service.py`, `pipeline/window.py` |
| Validation | four layers with machine-readable rejection reasons | `validation/` |
| Domain | typed locations, observations, windows, run summaries | `models/` |
| API adapter | deterministic requests, strict response models, retries | `api/`, `resilience.py` |
| Persistence | schema, upserts, run tracking, watermarks, advisory lock | `db/` |

Two architectural rules are enforced by tests, not convention: the API package
never imports the database package, and HTTP is confined to the API/resilience
modules.

## Technologies

| Area | Choice |
| --- | --- |
| Language | Python 3.12+ (developed on 3.14), fully type-annotated, ships `py.typed` |
| HTTP | `httpx` (sync client, per-phase timeouts, connection pooling) |
| Retries | `tenacity` with a custom full-jitter backoff and a request budget |
| Data validation | `pydantic` v2 strict models plus hand-written domain rules |
| Database | PostgreSQL 17 |
| DB access | SQLAlchemy 2.0 **Core** (no ORM) plus `psycopg` 3 |
| Migrations | Alembic, five revisions, `alembic check` for drift |
| Scheduling | APScheduler behind a thin wrapper; the one-shot CLI stays the contract |
| Logging | `structlog` (JSON and console renderers, recursive secret redaction) |
| CLI | `typer` |
| Config | `pydantic-settings` with explicit validation and production guards |
| Tests | `pytest` with `respx` for HTTP and `testcontainers` for PostgreSQL |
| Quality | `ruff` (lint + format), `mypy --strict`, `compileall` |
| Packaging | `hatchling`, locked dependency set in `uv.lock` |
| Runtime | Multi-stage Docker image, non-root (uid 10001), healthcheck |

## Reliability features

| Feature | Implementation | Evidence |
| --- | --- | --- |
| Single-run guarantee | `pg_try_advisory_lock(namespace, hashtext(key))`, two-key form | `tests/integration/test_advisory_lock.py` |
| Idempotent writes | `INSERT ... ON CONFLICT (natural key) DO UPDATE ... WHERE row_hash IS DISTINCT FROM EXCLUDED.row_hash` | `tests/integration/test_weather_repository.py` |
| True no-op on re-run | unchanged rows are not written at all | second demo run: `inserted=0, updated=0, unchanged=75` |
| Per-location transactions | one transaction per location; failures are isolated | `test_one_failing_location_does_not_roll_back_another` |
| Watermark correctness | the watermark advances in the same transaction as its rows | `tests/integration/test_watermark_store.py` |
| Crash safety | a kill before commit leaves the window collectable again | `test_crash_before_commit_leaves_the_window_collectable` |
| Retry taxonomy | only `RetryableUpstreamError` is retried; 4xx and schema errors never are | `tests/test_resilience.py`, `tests/test_api_open_meteo.py` |
| Timeout discipline | connect/read/write timeouts plus a whole-request budget across attempts | same files |
| Bounded blast radius | a failure budget ends a bad run; a run wall-clock budget stops new work | `tests/integration/test_pipeline_service.py` |
| Stale-run reaping | runs `running` for more than twice the run budget become `failed` | `tests/integration/test_run_maintenance.py` |
| Secret redaction | recursive, including inside exception tracebacks | `tests/test_logging.py` |
| Graceful shutdown | SIGINT/SIGTERM finish the in-flight collection, then stop | `tests/test_scheduler.py`, `scripts/scheduler_demo.py` |

## Data-quality features

**Four validation layers.** Transport (status, size, JSON), schema (strict typed
models that reject missing sections, ragged arrays, wrong types, changed units,
unparseable or out-of-order timestamps), domain (ranges, hour alignment, allowed
codes, source identity), and referential (location identity, coordinate drift,
window membership).

**Rejections are data, not exceptions.** Every rejected row carries a
machine-readable code and is written to `ingestion_run_errors` with the run,
location, phase, timestamp, and a bounded credential-masked payload sample - so
"why is this hour missing?" is answerable from SQL.

**A reject budget protects the table.** Above
`ADCP_INGEST_MAX_INVALID_ROW_RATIO` (default 25% of in-window rows) the location's
transaction rolls back entirely. Verified two ways in
[`samples/rejection-result.txt`](samples/rejection-result.txt): 1 of 5 rows (20%)
is a partial success; 1 of 3 rows (33%) fails the location and the run.

**Canonical storage.** Timestamps are normalised to hour-aligned UTC, numbers are
quantised to the column scale before hashing, and the row content hash is
deterministic: equivalent records hash identically, and a meaningful change to
the source data changes the hash.

**Provenance is preserved.** `weather_hourly` keeps the provider's grid
coordinates, the run that first and last saw each row, first/last collection
times, and a revision counter.

## Results

Everything below was produced by running the commands in this repository on a
single development machine (Windows, Python 3.14, local PostgreSQL 17 in
Docker). Nothing here is a capacity, throughput, or uptime claim.

| Measurement | Result | How it was produced |
| --- | --- | --- |
| Test suite | **576 passed, 1 skipped** (the opt-in live-API test), 0 failures | `pytest -q` |
| Coverage | **95.83%** line and branch on `src/adcp` (threshold 85%) | `pytest --cov` |
| Static analysis | `ruff check`, `ruff format --check`, `mypy --strict`, `compileall` all clean | CI and local |
| Migrations | `upgrade head` from empty, `downgrade -1`, and re-upgrade all clean; 5 revisions | `tests/integration/test_migrations.py` |
| Live collection | run 1 against an empty database: 288 received, 216 inserted (72 hours x 3 locations), 0 rejected, 0 retried | `adcp collect` |
| Idempotency proof | run 2 over the same window: **inserted 0, updated 0, unchanged 75** | `adcp collect` twice (also asserted by `scripts/demo.py`) |
| No duplicates | after both runs, 216 stored rows equal 216 distinct `(location_id, observed_at, source)` keys | `SELECT count(*), count(DISTINCT ...) FROM weather_hourly` |
| In-place revision handling | a changed hour updates the existing row and increments `revision_count` instead of inserting a duplicate | `tests/integration/test_weather_repository.py::test_changed_value_updates_the_row_in_place` |
| Synthetic load sanity check | 50 synthetic locations, fresh database: one run finished 50/50 with 1,200 rows inserted in about 2 s (concurrency 4); a second run hit one upstream HTTP 503, retried 15 requests, still committed the other 49 locations and reported `partial` | `python scripts/load_check.py --locations 50` against a scratch database (see the script docstring) |
| Scheduler | two cycles at a 15 s development cadence, no duplicate hours, clean shutdown | `python scripts/scheduler_demo.py --cycles 2 --interval-seconds 15` |
| Container | image builds, runs as uid 10001, healthcheck passes, no secrets in the image | `docker compose --profile app up --build` |

Repository statistics: 48 Python source modules (~6,800 lines), 45 test modules
(~6,600 lines) holding 576 tests, 5 database tables, 5 Alembic revisions, and a
locked dependency set of 55 packages.

## Testing

The suite is split so that unit tests need nothing but Python:

| Marker | What it proves | External services |
| --- | --- | --- |
| *(default)* | domain rules, request construction, retries, redaction, CLI wiring | none |
| `integration` | real SQL: unique key, no-op upsert, forecast/archive coexistence, watermarks, advisory lock, migrations, CLI against PostgreSQL | PostgreSQL |
| `live` | one opt-in smoke test against the real Open-Meteo API | network (opt-in) |

```bash
pytest -m "not integration"     # fast, no database
pytest -m integration           # PostgreSQL-backed
pytest --cov                    # everything, with the coverage gate
```

Integration tests use `ADCP_TEST_DATABASE_URL` when set, otherwise
Testcontainers, otherwise a disposable sibling of the Compose database; they
skip (rather than fail) when no PostgreSQL is reachable. A deliberately
deterministic six-run end-to-end scenario
(`tests/integration/test_end_to_end_scenario.py`) covers success, a no-op re-run,
changed data, partial failure, retry-then-success, and crash-before-commit.

## Example workflow

```bash
# 1. bring up PostgreSQL and apply the schema
docker compose up -d --wait postgres
adcp db upgrade

# 2. configure locations (idempotent)
python scripts/seed_locations.py

# 3. collect once
adcp collect

# 4. collect again - nothing changes
adcp collect --json
# look for: {"rows_inserted": 0, "rows_updated": 0, "rows_unchanged": 75}

# 5. audit what happened
docker exec adcp-postgres psql -U adcp -d adcp \
  -c "SELECT started_at, status, rows_inserted, rows_unchanged FROM ingestion_runs ORDER BY started_at DESC LIMIT 5;"

# 6. run it on a schedule (development cadence for the demo)
adcp schedule --interval-seconds 15 --run-once
```

The narrated 3-5 minute version, with the expected output at each step, is in
[`DEMO.md`](DEMO.md).
