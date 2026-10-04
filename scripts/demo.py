"""End-to-end walkthrough of the documented commands.

    python scripts/demo.py

It runs exactly what the README tells a reviewer to run - start PostgreSQL, apply
migrations, seed locations, collect twice - and then queries the database to show
the run history and the stored observations. The point of the second collection is
the idempotency proof: it must insert nothing.

Requirements: Docker (unless ``--skip-docker`` and PostgreSQL is already running),
a free-tier-friendly number of locations, and network access to Open-Meteo.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from typing import Any

import sqlalchemy as sa

from adcp.config import Settings
from adcp.db.engine import create_engine_from_settings, mask_engine_url


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the five-minute ADCP demo.")
    parser.add_argument(
        "--skip-docker",
        action="store_true",
        help="Assume PostgreSQL is already running (skip `docker compose up`).",
    )
    parser.add_argument(
        "--skip-collect",
        action="store_true",
        help="Skip the two live collections (no API calls).",
    )
    return parser.parse_args()


def run_command(command: list[str], *, capture: bool = False) -> subprocess.CompletedProcess[str]:
    """Run a documented command, echoing it so the demo is self-documenting."""
    print(f"$ {' '.join(command)}", flush=True)
    completed = subprocess.run(
        command,
        check=False,
        capture_output=capture,
        text=True,
    )
    if completed.returncode != 0:
        print(
            f"  command failed with exit code {completed.returncode}",
            file=sys.stderr,
        )
        if capture and completed.stderr:
            print(completed.stderr.strip(), file=sys.stderr)
        raise SystemExit(completed.returncode)
    return completed


def collect(*, capture: bool) -> tuple[dict[str, Any] | None, str]:
    """Run one collection, returning its parsed JSON summary."""
    completed = run_command(
        [sys.executable, "-m", "adcp", "collect", "--json"],
        capture=True,
    )
    print(completed.stdout.strip() if not capture else "  (summary parsed below)")
    try:
        return json.loads(completed.stdout), completed.stdout
    except json.JSONDecodeError:
        return None, completed.stdout


def show_run_history(engine: Any, *, limit: int = 3) -> None:
    statement = sa.text(
        "SELECT started_at, status, locations_succeeded || '/' || locations_total AS locations,"
        " rows_received, rows_inserted, rows_updated, rows_unchanged, rows_rejected, duration_ms"
        " FROM ingestion_runs ORDER BY started_at DESC LIMIT :limit",
    )
    with engine.connect() as connection:
        rows = connection.execute(statement, {"limit": limit}).all()
    print("  recent runs:")
    for row in rows:
        print(
            f"    {row[0]:%Y-%m-%d %H:%M:%S} {row[1]:<9} locations={row[2]:<4} "
            f"received={row[3]:<4} inserted={row[4]:<4} updated={row[5]:<3} "
            f"unchanged={row[6]:<4} rejected={row[7]:<2} {row[8]}ms",
        )


def show_observations(engine: Any) -> int:
    statement = sa.text(
        "SELECT l.slug, w.source, count(*) AS rows, min(w.observed_at) AS oldest,"
        " max(w.observed_at) AS newest, coalesce(sum(w.revision_count), 0) AS revisions"
        " FROM weather_hourly w JOIN locations l ON l.id = w.location_id"
        " GROUP BY l.slug, w.source ORDER BY l.slug",
    )
    with engine.connect() as connection:
        rows = connection.execute(statement).all()
        sample = connection.execute(
            sa.text(
                "SELECT l.slug, w.observed_at, w.temperature_2m, w.relative_humidity_2m,"
                " w.precipitation, w.wind_speed_10m"
                " FROM weather_hourly w JOIN locations l ON l.id = w.location_id"
                " ORDER BY w.observed_at DESC LIMIT 5",
            ),
        ).all()
        total = int(connection.execute(sa.text("SELECT count(*) FROM weather_hourly")).scalar_one())
    print("  stored observations:")
    for row in rows:
        print(
            f"    {row[0]:<14} {row[1]:<8} rows={row[2]:<4} {row[3]:%Y-%m-%dT%H:%M} -> "
            f"{row[4]:%Y-%m-%dT%H:%M} revisions={row[5]}",
        )
    print("  newest rows:")
    for row in sample:
        print(
            f"    {row[0]:<14} {row[1]:%Y-%m-%dT%H:%M} "
            f"temp={row[2]} humidity={row[3]} precip={row[4]} wind={row[5]}",
        )
    return total


def main() -> int:
    arguments = parse_arguments()
    settings = Settings()
    engine = create_engine_from_settings(settings)
    failures: list[str] = []
    try:
        print(f"database: {mask_engine_url(engine)}")
        if not arguments.skip_docker:
            run_command(["docker", "compose", "up", "-d", "--wait", "postgres"])

        run_command([sys.executable, "-m", "adcp", "db", "upgrade"])
        run_command([sys.executable, "-m", "adcp", "db", "current", "--check"])
        run_command([sys.executable, "scripts/seed_locations.py"])

        if arguments.skip_collect:
            print("\n(skipping the live collections)")
            show_run_history(engine)
            show_observations(engine)
            print("\nDEMO RESULT: SKIPPED COLLECTION")
            return 0

        print("\n--- run 1 ---")
        first, _ = collect(capture=False)
        rows_after_first = show_observations(engine)
        show_run_history(engine)

        print("\n--- run 2 (idempotency) ---")
        second, _ = collect(capture=False)
        rows_after_second = show_observations(engine)
        show_run_history(engine)

        if first is None or second is None:
            failures.append("could not parse the collection summaries")
        else:
            if not first["rows_accepted"]:
                failures.append("the first run accepted no observations")
            if second["rows_inserted"] or second["rows_updated"]:
                failures.append(
                    f"the second run was not a no-op: "
                    f"inserted={second['rows_inserted']} updated={second['rows_updated']}",
                )
            if not second["rows_unchanged"]:
                failures.append("the second run re-observed nothing, so idempotency is unproven")
        if rows_after_first is not None and rows_after_first != rows_after_second:
            failures.append(
                f"the second run changed the row count: {rows_after_first} -> {rows_after_second}",
            )

        print("")
        print(
            f"run 1: inserted={None if first is None else first['rows_inserted']} "
            f"received={None if first is None else first['rows_received']}"
        )
        print(
            f"run 2: inserted={None if second is None else second['rows_inserted']} "
            f"updated={None if second is None else second['rows_updated']} "
            f"unchanged={None if second is None else second['rows_unchanged']}"
        )
        print(f"stored rows after run 2: {rows_after_second}")
        print("DEMO RESULT:", "PASS" if not failures else f"FAIL -> {failures}")
        return 0 if not failures else 1
    finally:
        engine.dispose()


if __name__ == "__main__":
    raise SystemExit(main())
