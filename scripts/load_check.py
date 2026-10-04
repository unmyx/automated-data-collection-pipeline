"""Opt-in sanity check: collect many locations in one run and report the cost.

    python scripts/load_check.py --locations 50 --concurrency 4

This is the PLAN's load/sanity check. It creates synthetic ``load-NNN`` locations,
runs one real collection with the configured concurrency, and reports wall-clock
time, per-location cost, and whether the run finished inside an hour. It is never
part of the test suite: it calls the public API and writes rows.

Point it at a scratch database with ``ADCP_DATABASE_URL`` if you do not want the
data in your development database:

    ADCP_DATABASE_URL=postgresql+psycopg://adcp:pw@localhost:55432/adcp_load \\
        python scripts/load_check.py --locations 50
"""

from __future__ import annotations

import argparse
import statistics
import sys
import time
from decimal import Decimal

from sqlalchemy.engine import Engine

from adcp.cli.collect_cmd import run_collection_once
from adcp.config import Settings
from adcp.db.engine import create_engine_from_settings, mask_engine_url
from adcp.db.repository import LocationRepository

HOUR_SECONDS = 3_600


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect many locations once and report cost.")
    parser.add_argument("--locations", type=int, default=50)
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument("--lookback-hours", type=int, default=24)
    parser.add_argument("--overlap-hours", type=int, default=0)
    parser.add_argument(
        "--skip-seed",
        action="store_true",
        help="Reuse locations that already exist instead of creating synthetic ones.",
    )
    return parser.parse_args()


def seed(engine: Engine, count: int, *, skip: bool) -> list[str]:
    if skip:
        return [f"load-{index:03d}" for index in range(count)]
    repository = LocationRepository(engine)
    existing = {record.slug: record for record in repository.list_all()}
    for index in range(count):
        slug = f"load-{index:03d}"
        record = existing.get(slug)
        if record is not None:
            # The tool owns these synthetic locations. Re-enable one that an
            # earlier run (or `adcp db` maintenance) deactivated, so the check is
            # repeatable instead of failing with "unknown or inactive location".
            if not record.is_active:
                repository.set_active(slug, is_active=True)
            continue
        repository.create(
            slug=slug,
            name=f"Load {index:03d}",
            latitude=Decimal("35.000000") + Decimal(index) / Decimal("100"),
            longitude=Decimal("10.000000") + Decimal(index) / Decimal("100"),
        )
    return [f"load-{index:03d}" for index in range(count)]


def main() -> int:
    arguments = parse_arguments()
    settings = Settings().with_overrides(
        ingest_lookback_hours=arguments.lookback_hours,
        ingest_overlap_hours=arguments.overlap_hours,
        open_meteo_max_concurrency=arguments.concurrency,
    )
    engine = create_engine_from_settings(settings)
    try:
        print(f"database: {mask_engine_url(engine)}", file=sys.stderr)
        slugs = seed(engine, arguments.locations, skip=arguments.skip_seed)
        started = time.perf_counter()
        summary = run_collection_once(settings, trigger="cli", slugs=slugs)
        elapsed = time.perf_counter() - started

        durations = [result.duration_ms for result in summary.results]
        per_location = statistics.mean(durations) if durations else 0.0
        print("", file=sys.stderr)
        print(f"locations requested : {summary.counts.locations_total}", file=sys.stderr)
        print(f"locations succeeded : {summary.counts.locations_succeeded}", file=sys.stderr)
        print(f"locations failed    : {summary.counts.locations_failed}", file=sys.stderr)
        print(f"rows received       : {summary.counts.rows_received}", file=sys.stderr)
        print(f"rows inserted       : {summary.counts.rows_inserted}", file=sys.stderr)
        print(f"rows unchanged      : {summary.counts.rows_unchanged}", file=sys.stderr)
        print(f"rows rejected       : {summary.counts.rows_rejected}", file=sys.stderr)
        print(
            f"requests            : {summary.counts.requests_made} made, "
            f"{summary.counts.requests_retried} retried",
            file=sys.stderr,
        )
        print(f"wall clock          : {elapsed:.1f}s", file=sys.stderr)
        print(f"mean per location   : {per_location:.0f}ms", file=sys.stderr)
        print(f"concurrency         : {arguments.concurrency}", file=sys.stderr)
        print(f"status              : {summary.status.value}", file=sys.stderr)

        failures: list[str] = []
        if summary.counts.locations_failed:
            failures.append(f"{summary.counts.locations_failed} location(s) failed")
        if elapsed > HOUR_SECONDS:
            failures.append(f"the run took {elapsed:.0f}s, longer than an hour")
        print("LOAD CHECK:", "PASS" if not failures else f"FAIL -> {failures}")
        return 0 if not failures else 1
    finally:
        engine.dispose()


if __name__ == "__main__":
    raise SystemExit(main())
