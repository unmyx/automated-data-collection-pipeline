# What this project demonstrates for a client

This page explains the project in the terms a client actually asks about: what
problem it solves, what work it represents, why the engineering choices matter,
and what you receive.

Technical detail lives in [`PLAN.md`](PLAN.md) (design), [`ARCHITECTURE.md`](ARCHITECTURE.md)
(diagrams), and [`CASE_STUDY.md`](CASE_STUDY.md) (measured results). Nothing here
is client work or a production deployment: this is an independent project built
on public Open-Meteo data with synthetic locations.

---

## The client problem this pattern solves

Every data team eventually owns some version of this job: *call an external API
on a schedule, keep the result in a database, and be able to trust it.*

The painful parts are almost never the first successful run. They are:

- the run that overlaps with the previous one and inserts everything twice;
- the API that revises data you already stored, so your table quietly goes stale;
- the night the job dies halfway through and nobody knows which accounts or
  locations were affected;
- the day the provider changes a field type and the pipeline writes garbage,
  or nothing at all, without saying why;
- the audit question months later: "was this row collected, or edited?"

This project is a complete, working answer to that pattern, using a public
weather API as the concrete source.

## What this system automates

| Before | With this service |
| --- | --- |
| A cron job or a script someone has to babysit | One command (`adcp collect`) that is safe to run at any time, plus `adcp schedule` for hourly operation |
| Duplicate rows after a retry or an overlap | A natural-key upsert that makes re-runs a true no-op |
| Stored data that goes stale when the source revises it | A watermark plus an overlap window that re-reads recent hours and updates changed values in place |
| Quiet failures and manual spot checks | A run audit table, per-error records, structured logs, and freshness queries |
| Unvalidated rows landing in production tables | Four validation layers with a reject budget that rolls a bad payload back |
| "Ask the person who wrote it" | A documented CLI, runbook, architecture diagrams, and an automated test suite |

## What I implemented

About 6,800 lines of typed Python across 48 source modules, plus 45 test modules
(another ~6,600 lines) and 576 tests - built milestone by milestone, with each
milestone's exit criteria verified before the next started:

1. **Scaffold and design** - packaging, typed configuration, structured logging,
   exit-code contract, Docker/Compose, CI, and an 18-section design document.
2. **Database foundation** - PostgreSQL schema in Alembic (5 revisions, 5
   tables), SQLAlchemy 2.0 Core repositories, connection handling, advisory
   locking, and a database CLI.
3. **API layer** - an `httpx` client for Open-Meteo with deterministic request
   construction, strict response models, per-phase timeouts, a retry taxonomy
   with jittered backoff, and mocked-HTTP tests.
4. **Collection core** - the four validation layers, canonical normalisation and
   content hashing, the idempotent upsert, per-location transactions,
   watermarks, rejection tracking, run accounting, and `adcp collect`.
5. **Scheduling** - a thin scheduler that reuses the same collection service,
   with overlap safety, error isolation, structured operational events, and
   graceful shutdown.
6. **Hardening** - end-to-end error handling, exit-code consistency, redaction,
   a stale-run reaper, retention pruning, a run wall-clock budget, and a
   deterministic six-run end-to-end scenario.

## Why the design is reliable

The reliability comes from a small number of decisions, each of which is
independently tested:

**Idempotency, not optimism.** The natural key is
`(location_id, observed_at, source)` and each row carries a content hash. The
upsert only writes when the hash actually differs, so re-running any window - by
accident, on purpose, or after a crash - cannot duplicate or needlessly rewrite
data. This is what makes "just run it again" a safe recovery procedure.

**One transaction per location.** A bad location rolls back only its own rows and
its own watermark; everything that already committed stays committed, and the
run is recorded as partial. One broken account or region does not cost you the
whole batch.

**Watermarks that cannot lie.** The cursor advances in the same transaction as
the rows it describes. A process killed before commit leaves the window
collectable, so the next run simply re-collects it.

**Bounded everything.** Per-phase timeouts, a whole-request budget across
retries, jittered backoff, a per-location reject budget, a run-level failure
budget, and a run wall-clock budget. A bad provider day turns into one fast,
auditable failure instead of an unbounded hang.

**Retry only what can succeed.** Timeouts, connection errors, 5xx, and rate
limits (honouring `Retry-After`) are retried. A 4xx, malformed JSON, or a schema
violation never is, because waiting cannot fix a wrong request.

**Observability from the first run.** Every run, every failure, and every
rejection is a database row, and every log line of a run carries its `run_id`.
Credentials are redacted recursively, including inside tracebacks.

**Proof, not assertions.** 576 automated tests, 95.8% coverage, migrations verified
from an empty database, and integration tests that exercise real PostgreSQL for
the behaviours mocks cannot prove (unique keys, no-op upserts, advisory locks,
watermark semantics).

## What a client receives

If this project were adapted to your source and destination, the deliverable is:

- a working service with a one-shot command and a scheduler, packaged as a
  typed Python package and a non-root container image;
- the database schema as versioned, reversible migrations - not a hand-run
  script;
- a run/audit model so "did it run, and what failed?" is a SQL query;
- idempotent, incremental writes, so recovery is a re-run rather than a manual
  repair;
- structured logs, documented exit codes, and a runbook for on-call use;
- an automated test suite with unit and real-database integration coverage,
  wired into CI;
- documentation that a new engineer can follow: architecture diagrams, a design
  document, a demo script, and example queries.

## How this transfers to other sources

Nothing in the core depends on Open-Meteo or on weather. The pieces that would
change for a different API are the adapter (`api/`), the wire models, and the
domain rules; the lock, window planning, validation pipeline, idempotent upsert,
run tracking, watermarks, scheduler, CLI, logging, and tests all stay. That is
the point of the layering: swapping the source is a bounded, testable change,
not a rewrite.

---

## Publishing checklist (GitHub / Upwork assets)

Everything below is derived from this repository and can be produced without
inventing anything. Every number is reproducible with the commands in the README.

| Asset | Content |
| --- | --- |
| Repository name | `adcp` (or `automated-data-collection-pipeline` if a descriptive name is preferred) |
| Repository description | "Idempotent, incremental ingestion of hourly weather data from Open-Meteo into PostgreSQL - retries, validation, watermarks, run tracking, scheduling, and automated tests." |
| Topics | `data-engineering`, `etl`, `ingestion`, `postgresql`, `python`, `sqlalchemy`, `alembic`, `open-meteo`, `idempotency`, `observability`, `pytest` |
| GitHub project reference | `https://github.com/depduris/adcp` - keep it in sync with `pyproject.toml` `[project.urls]`, the README badge URLs, and `.github/workflows/ci.yml` if the repository is renamed |
| Social preview | The Mermaid architecture diagram from `docs/ARCHITECTURE.md` section 1, exported as a still |
| CV line | "Built an idempotent, watermark-driven ingestion service (Python, SQLAlchemy, PostgreSQL, Alembic) with retries, four-layer validation, per-location transactions, run auditing, scheduling, and 576 automated tests at 95.8% coverage." |
| Short demo video (3-5 min) | Walk the steps in [`DEMO.md`](DEMO.md); the two-collection idempotency proof is the closing shot |
| Written case study | [`CASE_STUDY.md`](CASE_STUDY.md), optionally exported to PDF |

### Upwork portfolio entry

**Title.** Idempotent hourly API-to-PostgreSQL ingestion pipeline (Python,
PostgreSQL)

**Role.** Data Engineer - solo, independent project (design, implementation, and
testing). Not client work.

**Description (574 characters; 581 including the line breaks as shown).**

```
Independent engineering project: a Python service that collects hourly weather
data from the public Open-Meteo REST API into PostgreSQL. It demonstrates
production-style ingestion: typed REST/JSON parsing, per-phase timeouts, retries
with jittered backoff, four validation layers, idempotent upserts keyed on
(location, hour, source), incremental watermarks, per-location transactions,
run/error tracking, structured JSON logs, scheduling with graceful shutdown, and
576 automated tests at 95.8% coverage. Runs locally with Docker Compose; public
data only, not client work.
```

**Five relevant skills.**

1. Python 3.12+ - typed API clients, dataclasses, packaging
2. PostgreSQL and SQL schema design - SQLAlchemy 2.0 Core, Alembic migrations, upserts, constraints, indexes
3. ETL and data-ingestion pipelines - idempotency, incremental watermarks, content hashing, per-location transactions
4. REST API integration - `httpx`, retry classification, backoff, timeouts, rate-limit handling
5. Data quality and automated testing - pydantic validation, pytest, testcontainers, CI, Docker Compose

**Recommended screenshots** (all reproducible with the README commands).

| # | Screenshot | Command that produces it |
| --- | --- | --- |
| 1 | `adcp collect` summary - the run result table | `adcp collect` |
| 2 | Idempotent second run - `inserted=0, updated=0, unchanged=N` | `adcp collect` twice |
| 3 | Run history and counters in PostgreSQL | query 3 in `docs/queries.sql` |
| 4 | Freshness per location/source | query 1 in `docs/queries.sql` |
| 5 | Failure digest with rejection reasons | query 8 in `docs/queries.sql`, or `docs/samples/rejection-result.txt` |
| 6 | Structured JSON retry log (503, backoff, success) | `docs/samples/retry-log.jsonl` |
| 7 | Architecture diagram | render `docs/ARCHITECTURE.md` section 1 |
| 8 | Green CI run (lint, types, tests, container) | the repository's Actions tab after publishing |

Boundaries to keep in mind when publishing: it is an independent project, it is
not client work, and no uptime, revenue, or scale figure should be attached to
it.
