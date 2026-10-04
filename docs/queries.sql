-- =============================================================================
-- ADCP example queries
--
-- A small, runnable analytics pack over the five tables in docs/PLAN.md
-- section 5. Every statement is read-only and safe to run against any ADCP
-- database, including an empty one.
--
-- Run the whole file with psql:
--
--   docker exec -i adcp-postgres psql -U adcp -d adcp < docs/queries.sql
--   # PowerShell: Get-Content docs/queries.sql | docker exec -i adcp-postgres psql -U adcp -d adcp
--
-- The captured results used in the README live in docs/samples/.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- 1. Freshness: how far behind is each location/source?
--
-- The watermark is the newest hour the pipeline has committed. "hours_behind"
-- is measured against the current hour, so a healthy hourly job stays at 1-2.
-- -----------------------------------------------------------------------------
SELECT l.slug,
       m.source,
       m.last_observed_at,
       round(
           extract(epoch FROM date_trunc('hour', now()) - m.last_observed_at) / 3600
       )::int AS hours_behind,
       m.updated_at
FROM ingestion_watermarks m
JOIN locations l ON l.id = m.location_id
ORDER BY hours_behind DESC, l.slug;


-- -----------------------------------------------------------------------------
-- 2. Stored observations per location and source.
--
-- Counts, coverage window, and accumulated revisions - the one-line answer to
-- "what do we actually have?".
-- -----------------------------------------------------------------------------
SELECT l.slug,
       w.source,
       count(*)                            AS rows,
       min(w.observed_at)::date            AS first_day,
       max(w.observed_at)::date            AS last_day,
       count(*) FILTER (WHERE w.revision_count > 0) AS revised_rows,
       coalesce(sum(w.revision_count), 0)  AS revisions
FROM weather_hourly w
JOIN locations l ON l.id = w.location_id
GROUP BY l.slug, w.source
ORDER BY l.slug, w.source;


-- -----------------------------------------------------------------------------
-- 3. Run history.
--
-- One row per collection, newest first. This is the table an operator reads
-- first when asking "did last night's job run?".
-- -----------------------------------------------------------------------------
SELECT started_at,
       finished_at,
       status,
       trigger,
       locations_succeeded || '/' || locations_total AS locations,
       rows_received,
       rows_inserted,
       rows_updated,
       rows_unchanged,
       rows_rejected,
       error_count,
       duration_ms
FROM ingestion_runs
ORDER BY started_at DESC
LIMIT 10;


-- -----------------------------------------------------------------------------
-- 4. Run outcome by day.
--
-- A weekday-over-weekday view of reliability. Anything that is not 'succeeded'
-- deserves a look at query 8.
-- -----------------------------------------------------------------------------
SELECT started_at::date AS day,
       count(*) FILTER (WHERE status = 'succeeded') AS succeeded,
       count(*) FILTER (WHERE status = 'partial')   AS partial,
       count(*) FILTER (WHERE status = 'failed')    AS failed,
       count(*) FILTER (WHERE status = 'skipped')   AS skipped,
       count(*)                                     AS runs,
       round(avg(duration_ms))                      AS avg_duration_ms
FROM ingestion_runs
GROUP BY started_at::date
ORDER BY day DESC;


-- -----------------------------------------------------------------------------
-- 5. Forecast revision churn.
--
-- The rows whose values changed after the first write. This is what the
-- overlap window exists to catch: forecast models revise, and the fact table
-- keeps the newest view while revision_count records how often it moved.
-- -----------------------------------------------------------------------------
SELECT l.slug,
       w.source,
       w.observed_at,
       w.revision_count,
       w.first_collected_at,
       w.last_collected_at
FROM weather_hourly w
JOIN locations l ON l.id = w.location_id
WHERE w.revision_count > 0
ORDER BY w.revision_count DESC, w.observed_at DESC
LIMIT 25;


-- -----------------------------------------------------------------------------
-- 6. Forecast and archive coexist for the same hour.
--
-- `source` is part of the natural key, so the same location/hour can hold a
-- forecast row and an archive row side by side. Future backfills exploit this.
-- -----------------------------------------------------------------------------
SELECT l.slug,
       w.observed_at,
       count(DISTINCT w.source) AS sources,
       array_agg(DISTINCT w.source ORDER BY w.source) AS which
FROM weather_hourly w
JOIN locations l ON l.id = w.location_id
GROUP BY l.slug, w.observed_at
HAVING count(DISTINCT w.source) > 1
ORDER BY w.observed_at DESC
LIMIT 25;


-- -----------------------------------------------------------------------------
-- 7. Gap finder: hours missing from the last 48 hours.
--
-- Compares expected hourly points against what is stored, per location. An
-- empty result is the healthy case.
-- -----------------------------------------------------------------------------
WITH expected AS (
    SELECT l.id AS location_id,
           l.slug,
           hour_start
    FROM locations l
    CROSS JOIN generate_series(
        date_trunc('hour', now()) - interval '48 hours',
        date_trunc('hour', now()) - interval '1 hour',
        interval '1 hour'
    ) AS hour_start
    WHERE l.is_active
)
SELECT e.slug,
       e.hour_start AS missing_hour
FROM expected e
LEFT JOIN weather_hourly w
       ON w.location_id = e.location_id
      AND w.observed_at = e.hour_start
      AND w.source = 'forecast'
WHERE w.id IS NULL
ORDER BY e.slug, e.hour_start;


-- -----------------------------------------------------------------------------
-- 8. Failure digest.
--
-- Why runs failed, grouped by phase and error type. The message column keeps a
-- representative example; payload samples live on the individual rows.
-- -----------------------------------------------------------------------------
SELECT phase,
       error_type,
       coalesce(error_code, '-') AS error_code,
       count(*)                  AS occurrences,
       min(occurred_at)          AS first_seen,
       max(occurred_at)          AS last_seen,
       (array_agg(message ORDER BY occurred_at DESC))[1] AS example
FROM ingestion_run_errors
GROUP BY phase, error_type, error_code
ORDER BY occurrences DESC, error_type;


-- -----------------------------------------------------------------------------
-- 9. Temperature extremes per location.
--
-- The coldest and hottest stored forecast hour for each location - a quick
-- sanity check that the data is plausible for the configured sites (Ushuaia
-- should be cold, Reykjavik mild, and so on).
-- -----------------------------------------------------------------------------
WITH ranked AS (
    SELECT l.slug,
           w.observed_at,
           w.temperature_2m,
           row_number() OVER (
               PARTITION BY l.slug ORDER BY w.temperature_2m ASC, w.observed_at
           ) AS coldest_rank,
           row_number() OVER (
               PARTITION BY l.slug ORDER BY w.temperature_2m DESC, w.observed_at
           ) AS hottest_rank
    FROM weather_hourly w
    JOIN locations l ON l.id = w.location_id
    WHERE w.temperature_2m IS NOT NULL
      AND w.source = 'forecast'
)
SELECT slug,
       max(observed_at) FILTER (WHERE coldest_rank = 1) AS coldest_at,
       max(temperature_2m) FILTER (WHERE coldest_rank = 1) AS coldest_c,
       max(observed_at) FILTER (WHERE hottest_rank = 1) AS hottest_at,
       max(temperature_2m) FILTER (WHERE hottest_rank = 1) AS hottest_c
FROM ranked
WHERE coldest_rank = 1 OR hottest_rank = 1
GROUP BY slug
ORDER BY slug;
