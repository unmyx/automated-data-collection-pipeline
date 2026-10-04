# Runbook

Operator procedures for the ADCP service. Every command here exists in the
current codebase and every query is read-only unless it explicitly says
otherwise.

Related: [ARCHITECTURE](ARCHITECTURE.md) · [queries.sql](queries.sql) ·
[PLAN section 11](PLAN.md#11-failure-and-partial-failure-behaviour)

---

## 1. Before you start

```bash
adcp config check          # exit 0 = the process has usable configuration
adcp db upgrade            # exit 0 = schema at head
adcp db current --check    # exit 0 = nothing pending
adcp db ping               # exit 0 = PostgreSQL reachable
```

An `.env` file is optional: the defaults in `.env.example` match the Compose
stack. Never paste a real credential into a shell history that is shared - the
service reads `ADCP_DATABASE_URL` from the environment or `.env`.

---

## 2. Normal operation

| Cadence | Action |
| --- | --- |
| Hourly | `adcp schedule` runs, or cron invokes `adcp collect` at a fixed minute |
| Daily | Check freshness for every active location (query 1 in [queries.sql](queries.sql)) |
| Weekly | Check the failure digest (query 8) and the run outcome by day (query 4) |
| Monthly | `adcp db prune` (dry run, then `--apply`) |

What healthy looks like:

- the newest run in `ingestion_runs` is `succeeded`;
- `hours_behind` in the freshness query is `1` or `2` for every active location;
- `weather_hourly.revision_count` is mostly `0` (a non-zero value means a
  forecast genuinely changed and was corrected in place);
- `ingestion_run_errors` has no new rows.

---

## 3. Triage

### 3.1 The scheduled job exited non-zero

Exit codes are the contract:

| Code | Meaning | First move |
| --- | --- | --- |
| 0 | success, or nothing to do | none |
| 1 | operational failure (run failed, or the database is unreachable) | read the run row, then section 3.2 |
| 2 | invalid configuration or usage | fix the flagged setting/flag; never auto-retry |
| 3 | partial success | read the run's error rows; data is committed for the successful locations |
| 130 | interrupted by the operator | none; re-run when convenient |

```bash
adcp db current --json       # reports current, head and pending as one JSON document
docker exec adcp-postgres psql -U adcp -d adcp \
  -c "SELECT * FROM ingestion_runs ORDER BY started_at DESC LIMIT 5"
```

The `psql` client ships inside the Compose PostgreSQL container, so the
`docker exec` form works without installing a local client.

### 3.2 A run failed or is partial

```sql
-- the last few runs, with counters
SELECT started_at, status, locations_succeeded || '/' || locations_total AS locations,
       rows_received, rows_inserted, rows_updated, rows_unchanged, rows_rejected,
       error_count, error_summary, duration_ms
FROM ingestion_runs
ORDER BY started_at DESC
LIMIT 5;

-- why the most recent failing run failed
SELECT phase, error_type, error_code, message, attempt, http_status, occurred_at
FROM ingestion_run_errors
WHERE run_id = (SELECT id FROM ingestion_runs ORDER BY started_at DESC LIMIT 1)
ORDER BY id;
```

Common causes and what they mean:

| `error_type` | Phase | Meaning | Action |
| --- | --- | --- | --- |
| `UpstreamTimeoutError` | `fetch` | connect/read timeout after all retries | check connectivity; re-run; widen the lookback if hours were missed |
| `UpstreamServerError` | `fetch` | 5xx from Open-Meteo after retries | upstream incident; re-run later |
| `UpstreamRateLimited` | `fetch` | 429 after honouring `Retry-After` | lower `ADCP_OPEN_METEO_MAX_CONCURRENCY` |
| `SchemaError` | `validate` | payload shape changed or is malformed | inspect `payload_sample`; the client is never allowed to guess |
| `OutOfRange` / `OutOfDomain` | `validate` | individual rows violated a domain rule | inspect `payload_sample`; if the whole model changed, review the rule |
| `RejectionBudgetExceeded` | `validate` | too many bad rows for this location | the location's transaction rolled back; investigate before re-running |
| `FailureBudgetExceeded` | `run` | too many locations failed, so the run stopped early | upstream incident; re-run when healthy |
| `RunTimeoutExceeded` | `run` | the run wall-clock budget was spent | committed locations survive; re-run collects the rest |

### 3.3 A location has no data / is stale

```sql
-- freshness per location and source
SELECT l.slug, m.source, m.last_observed_at,
       round(extract(epoch FROM date_trunc('hour', now()) - m.last_observed_at) / 3600)::int
           AS hours_behind
FROM ingestion_watermarks m
JOIN locations l ON l.id = m.location_id
ORDER BY hours_behind DESC;
```

1. Confirm the location is active: `SELECT slug, is_active FROM locations;`
2. Re-run just that location:
   `adcp collect --location <slug>`
3. If the gap is older than the current lookback window, widen it once:
   `adcp collect --location <slug> --lookback-hours 168`
4. If it still fails, read the error rows from section 3.2.

The re-run is safe by construction: the upsert's natural key and `row_hash`
comparison mean re-fetching an hour that is already stored writes nothing.

### 3.4 A run looks stuck

A process killed mid-run leaves its row `running`. The next collection
automatically reaps runs that have been `running` for more than twice
`ADCP_RUN_TIMEOUT_S`:

```sql
SELECT id, status, started_at, now() - started_at AS age, locations_total
FROM ingestion_runs
WHERE finished_at IS NULL
ORDER BY started_at;
```

No manual update is needed - start a collection and watch for
`ingest.runs.reaped` in the logs. Data committed by the dead run is intact
because each location commits independently.

### 3.5 Two runs at once

They cannot both collect. The PostgreSQL advisory lock is the authority, and the
loser logs `ingest.run.skipped` with `reason=lock_not_acquired` and exits 0. If
you see this on every tick, the previous run is genuinely still working: check
for `running` rows (section 3.4) and the `duration_ms` of recent runs.

---

## 4. Maintenance

### 4.1 Retention

```bash
adcp db prune                 # report only - the default
adcp db prune --json          # machine-readable report
adcp db prune --apply         # delete what the report listed
```

Defaults: `ingestion_runs` older than 365 days, `ingestion_run_errors` older than
90 days. Runs that observations still reference are kept and counted separately,
because `weather_hourly` records which run first and last saw each row. The fact
table is never pruned.

### 4.2 Schema changes

```bash
adcp db upgrade            # apply pending revisions
adcp db current            # show applied vs head
python -m alembic check    # detect drift between code and database
```

Never edit an applied migration; add a new one. `create_all()` is intentionally
not used anywhere - Alembic owns all DDL.

### 4.3 Logs

Set `ADCP_LOG_FORMAT=json` in any environment where a log collector is reading
stdout. `ADCP_LOG_LEVEL` is validated at startup. Secrets are redacted
recursively, including inside exception tracebacks: DSN passwords and
`apikey=...` query parameters become `***`.

Each line is one JSON object, so a run's lines can be isolated by filtering on
its `run_id`, and the bad news by filtering on `"level": "error"` or
`"level": "warning"` - with your log collector, `grep`, or any JSON tool.

---

## 5. Escalation checklist

1. Capture the failing run id and its `ingestion_run_errors` rows.
2. Capture the relevant structured log lines (they carry the same `run_id`).
3. Note whether the failure is upstream (`fetch`), payload (`validate`), or
   local (`persist` / `run`).
4. If `payload_sample` is present, it is already credential-masked and bounded -
   it can be shared as-is.
5. Re-run once the cause is understood. Never "fix" data by hand in
   `weather_hourly`; re-collect it instead, so the provenance columns stay true.
