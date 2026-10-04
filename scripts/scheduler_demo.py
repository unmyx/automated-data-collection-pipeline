"""Run the scheduler for a few development cycles, then stop it cleanly.

    python scripts/scheduler_demo.py --cycles 2 --interval-seconds 15

The script spawns the real ``adcp schedule`` command with the development
cadence, waits until the requested number of collections have completed, then
sends a Ctrl+Break (SIGBREAK on Windows, SIGTERM elsewhere) so the graceful
shutdown path is exercised rather than a hard kill. Finally it checks the
database for duplicate hours and reports what happened.

Requires at least one location (``python scripts/seed_locations.py``) and a
reachable Open-Meteo API.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import time
from collections.abc import Iterator
from typing import Any

import sqlalchemy as sa

from adcp.config import Settings
from adcp.db.engine import create_engine_from_settings, mask_engine_url

COMPLETED_EVENT = "scheduler.collection.completed"
STARTED_EVENT = "scheduler.started"
SHUTDOWN_EVENT = "scheduler.shutdown.requested"
STOPPED_EVENT = "scheduler.stopped"


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the scheduler for a few cycles.")
    parser.add_argument("--cycles", type=int, default=2, help="Collections to wait for.")
    parser.add_argument(
        "--interval-seconds",
        type=int,
        default=15,
        help="Development cadence passed to `adcp schedule`.",
    )
    parser.add_argument("--lookback-hours", type=int, default=2)
    parser.add_argument("--overlap-hours", type=int, default=1)
    parser.add_argument("--timeout-seconds", type=int, default=300)
    return parser.parse_args()


def active_locations(engine: Any) -> list[str]:
    with engine.connect() as connection:
        return list(
            connection.execute(
                sa.text("SELECT slug FROM locations WHERE is_active ORDER BY slug"),
            ).scalars(),
        )


def child_environment(arguments: argparse.Namespace) -> dict[str, str]:
    environment = dict(os.environ)
    environment.update(
        {
            "ADCP_SCHEDULER_ENABLED": "true",
            "ADCP_LOG_FORMAT": "json",
            "ADCP_INGEST_LOOKBACK_HOURS": str(arguments.lookback_hours),
            "ADCP_INGEST_OVERLAP_HOURS": str(arguments.overlap_hours),
        },
    )
    return environment


def start_child(arguments: argparse.Namespace) -> subprocess.Popen[str]:
    creation_flags = 0
    if sys.platform == "win32":  # a new process group so Ctrl+Break reaches only it
        creation_flags = subprocess.CREATE_NEW_PROCESS_GROUP
    return subprocess.Popen(
        [
            sys.executable,
            "-m",
            "adcp",
            "schedule",
            "--interval-seconds",
            str(arguments.interval_seconds),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=child_environment(arguments),
        creationflags=creation_flags,
    )


def json_events(lines: Iterator[str]) -> Iterator[dict[str, Any]]:
    for line in lines:
        stripped = line.strip()
        if not stripped.startswith("{"):
            continue
        try:
            yield json.loads(stripped)
        except json.JSONDecodeError:  # pragma: no cover - non-JSON noise
            continue


def request_stop(process: subprocess.Popen[str]) -> None:
    """Ask the child to stop the way an operator would."""
    break_event = getattr(signal, "CTRL_BREAK_EVENT", None)
    if sys.platform == "win32" and break_event is not None:
        process.send_signal(break_event)
    else:
        process.send_signal(signal.SIGTERM)


def watch_cycles(
    process: subprocess.Popen[str],
    arguments: argparse.Namespace,
) -> tuple[int, dict[str, int], bool]:
    """Read the child's logs, stopping it once enough cycles have completed.

    Reading continues until the child logs that it stopped (or the deadline
    passes), so the shutdown events are observed rather than left in the pipe.
    """
    completed = 0
    seen: dict[str, int] = {STARTED_EVENT: 0, SHUTDOWN_EVENT: 0, STOPPED_EVENT: 0}
    stop_requested = False
    deadline = time.monotonic() + arguments.timeout_seconds
    assert process.stderr is not None
    for event in json_events(process.stderr):
        name = str(event.get("event"))
        if name in seen:
            seen[name] += 1
        if name == COMPLETED_EVENT:
            completed += 1
            print(
                f"cycle {completed}: status={event.get('status')} "
                f"inserted={event.get('rows_inserted')} updated={event.get('rows_updated')} "
                f"unchanged={event.get('rows_unchanged')} rejected={event.get('rows_rejected')}",
                file=sys.stderr,
            )
            if completed >= arguments.cycles and not stop_requested:
                print("requesting graceful shutdown", file=sys.stderr)
                request_stop(process)
                stop_requested = True
        if stop_requested and seen[STOPPED_EVENT] >= 1:
            break
        if time.monotonic() > deadline:
            print("timed out waiting for the scheduler", file=sys.stderr)
            break
    return completed, seen, stop_requested


def database_report(engine: Any) -> dict[str, Any]:
    """Counts that prove the cycles wrote data without duplicating it."""
    with engine.connect() as connection:
        return {
            "duplicates": connection.execute(
                sa.text(
                    "SELECT count(*) FROM ("
                    "  SELECT location_id, source, observed_at, count(*) AS n"
                    "  FROM weather_hourly GROUP BY 1, 2, 3 HAVING count(*) > 1"
                    ") AS dupes",
                ),
            ).scalar_one(),
            "rows": connection.execute(sa.text("SELECT count(*) FROM weather_hourly")).scalar_one(),
            "runs": connection.execute(
                sa.text("SELECT status, count(*) FROM ingestion_runs GROUP BY 1 ORDER BY 1"),
            ).all(),
            "revisions": connection.execute(
                sa.text("SELECT coalesce(sum(revision_count), 0) FROM weather_hourly"),
            ).scalar_one(),
            "watermarks": connection.execute(
                sa.text("SELECT count(*) FROM ingestion_watermarks"),
            ).scalar_one(),
        }


def main() -> int:
    arguments = parse_arguments()
    engine = create_engine_from_settings(Settings())
    failures: list[str] = []
    try:
        locations = active_locations(engine)
        print(f"database: {mask_engine_url(engine)}", file=sys.stderr)
        print(f"active locations: {', '.join(locations) or '(none)'}", file=sys.stderr)
        if not locations:
            print("no active locations: run scripts/seed_locations.py first", file=sys.stderr)
            return 2

        process = start_child(arguments)
        print(
            f"scheduler started (pid {process.pid}); waiting for {arguments.cycles} cycle(s) "
            f"at {arguments.interval_seconds}s intervals",
            file=sys.stderr,
        )
        stop_requested = False
        try:
            completed, seen, stop_requested = watch_cycles(process, arguments)
        finally:
            if not stop_requested:
                print("requesting graceful shutdown", file=sys.stderr)
                request_stop(process)
            try:
                process.wait(timeout=120)
            except subprocess.TimeoutExpired:  # pragma: no cover - defensive
                process.kill()
                failures.append("the scheduler did not stop within 120s")

        if completed < arguments.cycles:
            failures.append(f"only {completed} of {arguments.cycles} cycles completed")
        if seen[SHUTDOWN_EVENT] < 1 or seen[STOPPED_EVENT] < 1:
            failures.append("the shutdown events were not logged")
        if process.returncode not in (0, None):
            failures.append(f"scheduler exited with {process.returncode}")

        report = database_report(engine)
        if report["duplicates"]:
            failures.append(f"{report['duplicates']} duplicated (location, source, hour) rows")

        print("", file=sys.stderr)
        print(f"cycles completed : {completed}/{arguments.cycles}", file=sys.stderr)
        print(f"rows stored      : {report['rows']}", file=sys.stderr)
        print(f"run statuses     : {report['runs']}", file=sys.stderr)
        print(f"revision total   : {report['revisions']}", file=sys.stderr)
        print(f"watermarks       : {report['watermarks']}", file=sys.stderr)
        print(f"duplicate hours  : {report['duplicates']}", file=sys.stderr)
        print(f"exit code        : {process.returncode}", file=sys.stderr)
        print(f"shutdown events  : {seen}", file=sys.stderr)
        print("")
        print("DEMO RESULT:", "PASS" if not failures else f"FAIL -> {failures}")
        return 0 if not failures else 1
    finally:
        engine.dispose()


if __name__ == "__main__":
    raise SystemExit(main())
