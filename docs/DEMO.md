# Five-minute demo

A script for walking a reviewer (or a client on a call) through the project in
3-5 minutes. Every command exists in this repository and is verified against the
current code. Timings are a *budget* - an estimate of how long each step should
take, not a measured benchmark.

Two ways to run it:

- **Narrated** - type the commands below and talk over them. This is the version
  for an interview or a screen-share.
- **Hands-off** - `python scripts/demo.py` performs steps 1-9 and prints a
  `DEMO RESULT: PASS` line. Use it when you only have time to show the outcome.

Related: [README](../README.md) · [ARCHITECTURE](ARCHITECTURE.md) ·
[PORTFOLIO](PORTFOLIO.md) · [samples](samples/)

---

## Before the call

```bash
python -m venv .venv
.venv\Scripts\activate                  # Windows;  source .venv/bin/activate elsewhere
pip install -e . --group dev
cp .env.example .env                    # Windows: copy .env.example .env
docker compose up -d --wait postgres
```

Warm the image and the database so the call starts on a healthy stack, and keep
`psql` open in a second window:

```bash
docker exec -it adcp-postgres psql -U adcp -d adcp
```

> On managed Windows machines that block freshly created `.exe` shims, use
> `python -m adcp <command>` everywhere `adcp <command>` appears below. Both forms
> are the same entry point.

---

## The demo

### ① Start the database — `0:00-0:20`

```bash
docker compose up -d --wait postgres
docker compose ps
```

**Say:** "PostgreSQL 17 with a healthcheck, published on host port 55432 so it
never collides with a database the developer already runs. The application
container talks to the service name `postgres`, not to `localhost`."

### ② Run the migrations — `0:20-0:40`

```bash
adcp db upgrade
adcp db current --check        # exit 0 = at head
```

**Say:** "Alembic owns the schema: five revisions, one per table. `db current
--check` is CI-friendly - it exits 1 when the database is behind, so a deploy
that forgot to migrate fails the pipeline instead of failing at 3 a.m."

### ③ Configure locations — `0:40-1:00`

```bash
python scripts/seed_locations.py
docker exec adcp-postgres psql -U adcp -d adcp -c "SELECT id, slug, latitude, longitude, is_active FROM locations ORDER BY slug;"
```

**Say:** "Locations are configuration-as-data, not hard-coded. The collector
reads them from PostgreSQL, so adding a city is an `INSERT`, not a deploy. The
seed script is idempotent - run it twice and nothing changes."

### ④ Run a collection — `1:00-1:40`

```bash
adcp collect
```

Expected shape of the output. The exact numbers depend on the current hour and on
how recently each location was collected - this is a real run against a fresh
local database:

```
Collection complete: succeeded
succeeded belgrade-rs              received=96 inserted=72 updated=0 unchanged=0 rejected=0 skipped=24
succeeded reykjavik-is             received=96 inserted=72 updated=0 unchanged=0 rejected=0 skipped=24
succeeded ushuaia-ar               received=96 inserted=72 updated=0 unchanged=0 rejected=0 skipped=24
run_id        <uuid>
locations     3 ok, 0 failed, 3 requested
rows          received 288, inserted 216, updated 0, unchanged 0, rejected 0, skipped 72
requests      3 made, 0 retried
summary       (all good)
```

Read it as: 96 hours arrived per location (the provider returns whole calendar
days), 72 of them fell inside this run's storage window (the default 72-hour
lookback), and the remaining 24 - the still-in-progress tail of today and the
hours before the lookback - were deliberately skipped rather than stored.

**Say:** "One command: takes a PostgreSQL advisory lock, reads each location's
watermark, fetches the window from Open-Meteo with bounded retries, validates
every row, upserts idempotently, and advances the watermark in the same
transaction. `skipped=24` is the part of the payload outside the storage window -
hours still in progress are not stored yet, and the next run will collect them."

### ⑤ Inspect the ingestion result — `1:40-2:00`

```bash
adcp collect --json
```

Look for `status`, `locations_succeeded`, `rows_inserted`, `rows_unchanged`, and
`rows_rejected` in the document.

**Say:** "The same run, machine-readable. Exit codes are part of the contract:
0 success, 1 operational failure, 2 configuration error, 3 partial success - so
cron and CI can branch on the result."

### ⑥ Query the stored data — `2:00-2:20`

```bash
docker exec adcp-postgres psql -U adcp -d adcp -c \
  "SELECT l.slug, w.source, count(*) AS rows, min(w.observed_at) AS oldest,
          max(w.observed_at) AS newest, sum(w.revision_count) AS revisions
     FROM weather_hourly w JOIN locations l ON l.id = w.location_id
    GROUP BY l.slug, w.source ORDER BY l.slug;"
```

**Say:** "Hourly weather in a normalised fact table, with the provider's grid
coordinates kept alongside for provenance. `source` is part of the natural key,
so a forecast and an archive observation of the same hour can coexist."

### ⑦ Run the collection again — `2:20-2:40`

```bash
adcp collect
```

**Say:** "Same command, seconds later. The watermark plus the overlap window
means it re-reads the newest hours - exactly what you want when a model revises
its forecast."

### ⑧ Prove there are no duplicates — `2:40-3:00`

```bash
docker exec adcp-postgres psql -U adcp -d adcp -c \
  "SELECT count(*) AS total, count(DISTINCT (location_id, observed_at, source)) AS distinct_keys
     FROM weather_hourly;"
```

```bash
docker exec adcp-postgres psql -U adcp -d adcp -c \
  "SELECT status, rows_received, rows_inserted, rows_updated, rows_unchanged
     FROM ingestion_runs ORDER BY started_at DESC LIMIT 2;"
```

**Say:** "`total` equals `distinct_keys`, so the unique constraint holds. The
second run reports `inserted=0, updated=0, unchanged=N` - the upsert's
`WHERE row_hash IS DISTINCT FROM` turns identical data into a true storage
no-op, not a rewrite. Re-running any window, at any time, by any number of
workers, cannot duplicate a row."

The committed version of this proof is in
[`samples/idempotent-rerun.txt`](samples/idempotent-rerun.txt).

### ⑨ Show the run history — `3:00-3:20`

```bash
docker exec adcp-postgres psql -U adcp -d adcp -c \
  "SELECT started_at, status, trigger, locations_succeeded || '/' || locations_total AS locations,
          rows_inserted, rows_unchanged, rows_rejected, duration_ms
     FROM ingestion_runs ORDER BY started_at DESC LIMIT 5;"
```

**Say:** "Every run is an audit record: what ran, when, how many locations
succeeded, and the full row accounting. When something does fail, the reason
lands in `ingestion_run_errors` with the phase, the location, the HTTP status,
and a credential-masked payload sample."

### ⑩ Start the scheduler in development mode — `3:20-3:50`

```bash
adcp schedule --interval-seconds 15 --run-once
```

**Say:** "`adcp schedule` is a thin wrapper: it decides *when*, and delegates
every collection to the same code path as the CLI. In production it runs hourly
at minute 7 UTC; `--interval-seconds` is a development cadence so a demo does not
wait an hour between cycles. `--run-once` collects immediately and then keeps the
schedule."

### ⑪ Show recurring execution and logging — `3:50-4:20`

```bash
python scripts/scheduler_demo.py --cycles 2 --interval-seconds 15
```

**Say:** "This runs the real command, waits for two collections, sends a graceful
shutdown signal, and then checks PostgreSQL for duplicate hours. You can see the
operational events in order: `scheduler.started`, `scheduler.next_run`,
`scheduler.collection.triggered`, `scheduler.collection.completed`,
`scheduler.shutdown.requested`, `scheduler.stopped`."

Two collections over the same window: the second inserts zero rows, so the
scheduler inherits the CLI's idempotency for free.

### ⑫ Review the repository — `4:20-5:00`

Open GitHub and walk through, in this order:

1. **README** - problem, architecture diagram, quickstart, CLI reference, the
   idempotency explanation, and captured sample output.
2. **`docs/ARCHITECTURE.md`** - component, data-flow, sequence, ER, and
   failure/retry diagrams.
3. **`docs/samples/`** - deterministic, committed outputs generated by
   `scripts/capture_samples.py`, so the documentation can be re-verified rather
   than trusted.
4. **`tests/`** - the unit suite runs with no database; the integration suite
   proves the SQL behaviour (unique key, no-op upsert, forecast/archive
   coexistence, watermark correctness, advisory-lock mutual exclusion).
5. **`.github/workflows/ci.yml`** - lint, format, strict types, byte-compile,
   Compose validation, migrations from empty, schema-drift detection, and the
   full suite with an enforced coverage threshold.
6. **`docs/PORTFOLIO.md`** and **`docs/CASE_STUDY.md`** - the same project
   explained in client language.

---

## If something goes wrong on the call

| Symptom | Fix |
| --- | --- |
| `adcp: command not found`, or an `.exe` blocked by policy | use `python -m adcp <command>` |
| `password authentication failed` | the container was recreated with a different password - `docker compose down` then `up -d --wait postgres` |
| Port 55432 already in use | set `POSTGRES_PORT` and the matching port in `ADCP_DATABASE_URL`, then `docker compose up -d --wait postgres` |
| Open-Meteo is slow or throttling | lower `ADCP_OPEN_METEO_MAX_CONCURRENCY`, or run `adcp collect --location belgrade-rs` for a single location |
| No rows appear | check `adcp db current --check`, then the run history query in ⑨ and the error rows for that `run_id` |
| You need a no-network demo | `python scripts/demo.py --skip-collect` shows the schema, run history, and stored rows without calling the API |

The full failure catalogue is in [`PLAN.md`](PLAN.md#11-failure-and-partial-failure-behaviour)
section 11 and [`RUNBOOK.md`](RUNBOOK.md).

---

## Hands-off version

```bash
python scripts/demo.py
```

It starts PostgreSQL, migrates, seeds, collects twice, queries the run history
and the stored observations, asserts that the second run inserted and updated
nothing, and prints `DEMO RESULT: PASS`. Add `--skip-docker` when PostgreSQL is
already running, or `--skip-collect` to run without network access.
