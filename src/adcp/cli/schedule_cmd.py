"""``adcp schedule`` - the long-running wrapper around ``adcp collect``.

The command validates configuration, checks the database, and then hands control
to :class:`adcp.scheduler.CollectionScheduler`, whose job body is the very same
``run_collection_once`` helper that ``adcp collect`` uses. Scheduling therefore
adds *when*, never *what*.

Exit codes:

======  ==================================================================
``0``   the scheduler stopped cleanly (SIGINT/SIGTERM)
``1``   startup failure that is operational (database unreachable)
``2``   invalid configuration (scheduling disabled, bad timezone/minute, schema behind head)
======  ==================================================================
"""

from __future__ import annotations

from typing import Annotated

import typer

from adcp.cli.collect_cmd import run_collection_once
from adcp.cli.common import emit_error, load_settings
from adcp.config import Settings
from adcp.db.migrations.runner import schema_revision
from adcp.errors import ConfigurationError, DatabaseError
from adcp.exit_codes import ExitCode
from adcp.scheduler import (
    MAX_INTERVAL_SECONDS,
    CollectionScheduler,
    SchedulerPlan,
    build_schedule,
)


def schedule(
    minute: Annotated[
        int | None,
        typer.Option(
            "--minute",
            min=0,
            max=59,
            help="Minute past the hour for the hourly schedule.",
        ),
    ] = None,
    timezone: Annotated[
        str | None,
        typer.Option("--timezone", help="IANA timezone the schedule is evaluated in."),
    ] = None,
    interval_seconds: Annotated[
        int | None,
        typer.Option(
            "--interval-seconds",
            min=1,
            max=MAX_INTERVAL_SECONDS,
            help="Development cadence: collect every N seconds instead of hourly.",
        ),
    ] = None,
    run_once: Annotated[
        bool,
        typer.Option("--run-once", help="Collect immediately, then keep the schedule."),
    ] = False,
) -> None:
    """Run the collection pipeline on a schedule until interrupted."""
    settings = load_settings()
    _require_scheduling_enabled(settings)
    plan = _build_plan(
        settings,
        minute=minute,
        timezone=timezone,
        interval_seconds=interval_seconds,
        run_once=run_once,
    )
    startup_failure = _preflight(settings)
    if startup_failure is not None:
        raise typer.Exit(startup_failure)

    scheduler = CollectionScheduler(
        plan,
        runner=lambda: run_collection_once(settings, trigger="scheduler"),
        skip_if_running=settings.scheduler_skip_if_running,
    )
    typer.echo(f"Scheduling: {plan.description} ({plan.timezone}); Ctrl+C to stop")
    scheduler.start()
    raise typer.Exit(ExitCode.OK)


def _require_scheduling_enabled(settings: Settings) -> None:
    if not settings.scheduler_enabled:
        emit_error(
            "scheduling is disabled: set ADCP_SCHEDULER_ENABLED=true "
            "(see .env.example) or run `adcp collect` for a single run",
        )
        raise typer.Exit(ExitCode.CONFIG_ERROR)


def _build_plan(
    settings: Settings,
    *,
    minute: int | None,
    timezone: str | None,
    interval_seconds: int | None,
    run_once: bool,
) -> SchedulerPlan:
    try:
        return build_schedule(
            settings,
            minute=minute,
            timezone=timezone,
            interval_seconds=interval_seconds,
            run_once=run_once,
        )
    except ConfigurationError as exc:
        emit_error(str(exc))
        raise typer.Exit(ExitCode.CONFIG_ERROR) from exc


def _preflight(settings: Settings) -> ExitCode | None:
    """Fail fast when the scheduler could not possibly collect anything."""
    try:
        revision = schema_revision(
            database_url=str(settings.database_url),
            connect_timeout_s=settings.db_connect_timeout_s,
        )
    except DatabaseError as exc:
        emit_error(f"cannot start the scheduler: {exc}")
        return ExitCode.FAILURE
    if not revision.is_current:
        emit_error(
            f"the database schema is behind head (at {revision.revision!r}, "
            f"head is {revision.head!r}); run `adcp db upgrade` first",
        )
        return ExitCode.CONFIG_ERROR
    return None


__all__ = ["schedule"]
