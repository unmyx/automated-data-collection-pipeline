"""``adcp collect`` - the one-shot collection command.

This is the primary contract of the whole project (PLAN section 7.2): a single
command that is safe to run at any time, by any scheduler, in any environment.
Scheduling is deliberately *not* part of it.

Exit codes follow PLAN section 11.4:

======  ==================================================================
``0``   full success, or nothing to do (lock held, no active locations)
``1``   operational failure (run failed, database unreachable)
``2``   invalid configuration or usage (unknown location, bad flag combination)
``3``   partial success (some rows written, something failed or was rejected)
======  ==================================================================
"""

from __future__ import annotations

from typing import Annotated, NoReturn

import typer
from sqlalchemy.engine import Engine

from adcp.api.open_meteo import OpenMeteoClient
from adcp.cli.common import emit_error, emit_fields, emit_json, load_settings
from adcp.config import Settings
from adcp.db.engine import create_engine_from_settings
from adcp.errors import AdcpError, ConfigurationError, DatabaseError
from adcp.exit_codes import ExitCode
from adcp.models.observation import ObservationSource
from adcp.models.run import RunStatus, RunSummary
from adcp.pipeline.service import CollectionService

MAX_LOOKBACK_OPTION = 92 * 24
MAX_OVERLAP_OPTION = 7 * 24


def exit_code_for(summary: RunSummary) -> ExitCode:
    """Map a run summary onto the documented exit codes."""
    if summary.status in {RunStatus.SUCCEEDED, RunStatus.SKIPPED}:
        return ExitCode.OK
    if summary.status is RunStatus.PARTIAL:
        return ExitCode.PARTIAL
    return ExitCode.FAILURE


def render_summary(summary: RunSummary) -> str:
    """Two-line-per-location human summary, ending with the run totals."""
    lines: list[str] = []
    for result in summary.results:
        detail = (
            f"received={result.rows_received} inserted={result.rows_inserted} "
            f"updated={result.rows_updated} unchanged={result.rows_unchanged} "
            f"rejected={result.rows_rejected} skipped={result.rows_skipped}"
        )
        lines.append(f"{result.status.value:<9} {result.slug:<24} {detail}")
        if result.error_message:
            lines.append(f"{'':<9} {result.error_type}: {result.error_message[:120]}")
    return "\n".join(lines)


def collect(
    location: Annotated[
        list[str] | None,
        typer.Option(
            "--location",
            "-l",
            help="Collect only these location slugs (repeatable).",
        ),
    ] = None,
    lookback_hours: Annotated[
        int | None,
        typer.Option(
            "--lookback-hours",
            min=1,
            max=MAX_LOOKBACK_OPTION,
            help="Override ADCP_INGEST_LOOKBACK_HOURS for this run.",
        ),
    ] = None,
    overlap_hours: Annotated[
        int | None,
        typer.Option(
            "--overlap-hours",
            min=0,
            max=MAX_OVERLAP_OPTION,
            help="Override ADCP_INGEST_OVERLAP_HOURS for this run.",
        ),
    ] = None,
    dry_run: Annotated[
        bool,
        typer.Option("--dry-run", help="Fetch and validate, but write nothing."),
    ] = False,
    as_json: Annotated[
        bool,
        typer.Option("--json", help="Emit machine-readable output."),
    ] = False,
) -> None:
    """Collect hourly observations for every active location, once."""
    settings = _settings_with_overrides(lookback_hours=lookback_hours, overlap_hours=overlap_hours)
    summary = _run_collection(settings, slugs=location, dry_run=dry_run, as_json=as_json)

    if as_json:
        emit_json(summary.as_dict())
    else:
        _render(summary, dry_run=dry_run)
    raise typer.Exit(exit_code_for(summary))


def _settings_with_overrides(
    *,
    lookback_hours: int | None,
    overlap_hours: int | None,
) -> Settings:
    settings = load_settings()
    overrides: dict[str, object] = {}
    if lookback_hours is not None:
        overrides["ingest_lookback_hours"] = lookback_hours
    if overlap_hours is not None:
        overrides["ingest_overlap_hours"] = overlap_hours
    if not overrides:
        return settings
    try:
        return settings.with_overrides(**overrides)
    except ValueError as exc:
        emit_error(f"invalid option combination: {exc}")
        raise typer.Exit(ExitCode.CONFIG_ERROR) from exc


def _run_collection(
    settings: Settings,
    *,
    slugs: list[str] | None,
    dry_run: bool,
    as_json: bool,
) -> RunSummary:
    try:
        return run_collection_once(settings, slugs=slugs, dry_run=dry_run, trigger="cli")
    except ConfigurationError as exc:
        _report(exc, as_json=as_json, exit_code=ExitCode.CONFIG_ERROR)
    except DatabaseError as exc:
        _report(exc, as_json=as_json, exit_code=ExitCode.FAILURE)
    except AdcpError as exc:
        _report(exc, as_json=as_json, exit_code=ExitCode.FAILURE)


def run_collection_once(
    settings: Settings,
    *,
    slugs: list[str] | None = None,
    dry_run: bool = False,
    source: ObservationSource = ObservationSource.FORECAST,
    trigger: str = "cli",
) -> RunSummary:
    """Run exactly one collection and return its summary.

    This is the composition root for a single run: it builds the engine and the
    Open-Meteo adapter, hands them to :class:`~adcp.pipeline.service.CollectionService`,
    and disposes them afterwards. ``adcp collect`` calls it directly, and
    ``adcp schedule`` passes it to the scheduler as the job body - which is how
    scheduled and manual runs are guaranteed to behave identically.
    """
    engine: Engine = create_engine_from_settings(settings)
    try:
        with OpenMeteoClient(settings) as weather_source:
            service = CollectionService(
                settings,
                source=weather_source,
                engine=engine,
                stats=weather_source.stats.snapshot,
            )
            return service.run(
                trigger=trigger,
                location_slugs=slugs,
                dry_run=dry_run,
                source=source,
            )
    finally:
        engine.dispose()


def _report(exc: AdcpError, *, as_json: bool, exit_code: ExitCode) -> NoReturn:
    """Report an expected failure and never return."""
    if as_json:
        emit_json(
            {
                "status": "error",
                "error_type": type(exc).__name__,
                "message": str(exc),
            },
        )
    else:
        emit_error(str(exc))
    raise typer.Exit(exit_code)


def _render(summary: RunSummary, *, dry_run: bool) -> None:
    heading = "Dry run (nothing written)" if dry_run else "Collection complete"
    colour = {
        RunStatus.SUCCEEDED: typer.colors.GREEN,
        RunStatus.SKIPPED: typer.colors.YELLOW,
        RunStatus.PARTIAL: typer.colors.YELLOW,
        RunStatus.FAILED: typer.colors.RED,
    }[summary.status]
    typer.secho(f"{heading}: {summary.status.value}", fg=colour)
    detail = render_summary(summary)
    if detail:
        typer.echo(detail)
    emit_fields(
        {
            "run_id": None if summary.run_id is None else str(summary.run_id),
            "locations": (
                f"{summary.counts.locations_succeeded} ok, "
                f"{summary.counts.locations_failed} failed, "
                f"{summary.counts.locations_total} requested"
            ),
            "rows": (
                f"received {summary.counts.rows_received}, "
                f"inserted {summary.counts.rows_inserted}, "
                f"updated {summary.counts.rows_updated}, "
                f"unchanged {summary.counts.rows_unchanged}, "
                f"rejected {summary.counts.rows_rejected}, "
                f"skipped {summary.counts.rows_skipped}"
            ),
            "requests": (
                f"{summary.counts.requests_made} made, {summary.counts.requests_retried} retried"
            ),
            "duration_ms": summary.duration_ms,
            "summary": summary.error_summary or "(all good)",
        },
    )


__all__ = ["collect", "exit_code_for", "render_summary", "run_collection_once"]
