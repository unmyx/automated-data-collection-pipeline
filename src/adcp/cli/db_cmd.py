"""``adcp db ...`` - database connectivity and schema management.

Exit codes follow ``docs/PLAN.md`` section 11.4: 0 success, 1 unreachable
database or failed migration, 2 invalid configuration (handled before any
connection is opened by :func:`adcp.cli.common.load_settings`).
"""

from __future__ import annotations

from typing import Annotated

import typer

from adcp.cli.common import emit_error, emit_fields, emit_json, load_settings
from adcp.db.engine import create_engine_from_settings, ping_database
from adcp.db.migrations.runner import schema_revision, upgrade
from adcp.db.run_tracker import RunTracker
from adcp.errors import DatabaseError
from adcp.exit_codes import ExitCode
from adcp.logging import mask_credentials_in_text

db_app = typer.Typer(
    name="db",
    help="Check database connectivity and manage the schema.",
    no_args_is_help=True,
)


def _report_failure(exc: DatabaseError, *, as_json: bool, command: str, database_url: str) -> None:
    """Report a database failure, always masking credentials.

    In JSON mode only the JSON document is written, so ``--json`` output stays
    machine-parseable; the human-readable line would otherwise be appended to the
    same stream by the test/console runner.
    """
    message = mask_credentials_in_text(str(exc))
    if as_json:
        emit_json(
            {
                "command": command,
                "database_url": database_url,
                "error": message,
                "error_type": exc.__class__.__name__,
                "status": "unreachable",
            },
        )
        return
    emit_error(message)


@db_app.command("ping")
def db_ping(
    as_json: Annotated[bool, typer.Option("--json", help="Emit machine-readable output.")] = False,
) -> None:
    """Check connectivity and report the server version."""
    settings = load_settings()
    database_url = settings.masked_database_url()
    engine = create_engine_from_settings(settings)
    try:
        result = ping_database(engine)
    except DatabaseError as exc:
        _report_failure(exc, as_json=as_json, command="db ping", database_url=database_url)
        raise typer.Exit(ExitCode.FAILURE) from exc
    finally:
        engine.dispose()

    payload = {
        "database": result.database,
        "database_url": database_url,
        "host": f"{engine.url.host}:{engine.url.port}" if engine.url.host else "local socket",
        "latency_ms": result.latency_ms,
        "server_version": result.server_version,
        "in_recovery": result.in_recovery,
        "status": "ok",
        "user": result.username,
    }
    if as_json:
        emit_json(payload)
    else:
        typer.secho("Database OK", fg=typer.colors.GREEN)
        emit_fields(
            {
                "host": payload["host"],
                "database": result.database,
                "user": result.username,
                "server": f"PostgreSQL {result.server_version}",
                "latency": f"{result.latency_ms} ms",
            },
        )
    raise typer.Exit(ExitCode.OK)


@db_app.command("upgrade")
def db_upgrade(
    revision: Annotated[
        str,
        typer.Option("--revision", "-r", help="Target revision to upgrade to."),
    ] = "head",
    as_json: Annotated[bool, typer.Option("--json", help="Emit machine-readable output.")] = False,
) -> None:
    """Apply pending schema migrations (Alembic ``upgrade``)."""
    settings = load_settings()
    database_url = settings.masked_database_url()
    raw_url = str(settings.database_url)
    timeout = settings.db_connect_timeout_s

    try:
        before = schema_revision(database_url=raw_url, connect_timeout_s=timeout)
        if not before.is_current:
            outcome = upgrade(
                database_url=raw_url,
                revision=revision,
                connect_timeout_s=timeout,
            )
            after = schema_revision(database_url=raw_url, connect_timeout_s=timeout)
            applied = list(outcome.applied)
        else:
            outcome = None
            after = before
            applied = []
    except DatabaseError as exc:
        _report_failure(exc, as_json=as_json, command="db upgrade", database_url=database_url)
        raise typer.Exit(ExitCode.FAILURE) from exc

    payload = {
        "applied": applied,
        "database_url": database_url,
        "head": after.head,
        "previous_revision": before.revision,
        "revision": after.revision,
        "status": "ok",
        "up_to_date": after.is_current,
    }
    if as_json:
        emit_json(payload)
    else:
        if applied:
            typer.secho(f"Applied {len(applied)} migration(s)", fg=typer.colors.GREEN)
        else:
            typer.secho("Schema already up to date", fg=typer.colors.GREEN)
        emit_fields(
            {
                "previous": before.revision or "(none)",
                "current": after.revision or "(none)",
                "head": after.head or "(none)",
                "applied": ", ".join(applied) if applied else "(nothing to do)",
            },
        )
    raise typer.Exit(ExitCode.OK)


@db_app.command("current")
def db_current(
    as_json: Annotated[bool, typer.Option("--json", help="Emit machine-readable output.")] = False,
    check: Annotated[
        bool,
        typer.Option("--check", help="Exit 1 when the schema is not at the head revision."),
    ] = False,
) -> None:
    """Show the schema revision applied to the database."""
    settings = load_settings()
    database_url = settings.masked_database_url()
    try:
        revision = schema_revision(
            database_url=str(settings.database_url),
            connect_timeout_s=settings.db_connect_timeout_s,
        )
    except DatabaseError as exc:
        _report_failure(exc, as_json=as_json, command="db current", database_url=database_url)
        raise typer.Exit(ExitCode.FAILURE) from exc

    payload = {
        "database_url": database_url,
        "head": revision.head,
        "pending": list(revision.pending),
        "revision": revision.revision,
        "status": "ok",
        "up_to_date": revision.is_current,
    }
    if as_json:
        emit_json(payload)
    else:
        rows: dict[str, object] = {
            "current": revision.revision or "(none)",
            "head": revision.head or "(none)",
            "pending": len(revision.pending),
        }
        if revision.is_current:
            typer.secho("Schema is up to date", fg=typer.colors.GREEN)
        else:
            typer.secho("Schema is behind head", fg=typer.colors.YELLOW)
            rows["next"] = revision.pending[0] if revision.pending else "(unknown)"
        emit_fields(rows)

    if check and not revision.is_current:
        raise typer.Exit(ExitCode.FAILURE)
    raise typer.Exit(ExitCode.OK)


@db_app.command("prune")
def db_prune(
    runs_older_than_days: Annotated[
        int,
        typer.Option(
            "--runs-older-than-days",
            min=1,
            help="Retention window for ingestion_runs (PLAN section 5.8: 365).",
        ),
    ] = 365,
    errors_older_than_days: Annotated[
        int,
        typer.Option(
            "--errors-older-than-days",
            min=1,
            help="Retention window for ingestion_run_errors (PLAN section 5.8: 90).",
        ),
    ] = 90,
    apply_changes: Annotated[
        bool,
        typer.Option(
            "--apply",
            help="Actually delete. Without it the command only reports what it would delete.",
        ),
    ] = False,
    as_json: Annotated[bool, typer.Option("--json", help="Emit machine-readable output.")] = False,
) -> None:
    """Apply the retention policy - report by default, delete with ``--apply``.

    Runs that observations still reference are kept (and counted) because the fact
    table records which run first and last saw each row; deleting them would
    destroy provenance. ``weather_hourly`` itself is never pruned.
    """
    settings = load_settings()
    database_url = settings.masked_database_url()
    engine = create_engine_from_settings(settings)
    try:
        summary = RunTracker(engine).prune(
            runs_older_than_days=runs_older_than_days,
            errors_older_than_days=errors_older_than_days,
            dry_run=not apply_changes,
        )
    except DatabaseError as exc:
        _report_failure(exc, as_json=as_json, command="db prune", database_url=database_url)
        raise typer.Exit(ExitCode.FAILURE) from exc
    finally:
        engine.dispose()

    payload = {
        "database_url": database_url,
        "dry_run": summary.dry_run,
        "runs_cutoff": summary.runs_cutoff,
        "runs_eligible": summary.runs_eligible,
        "runs_deleted": summary.runs_deleted,
        "runs_kept_by_provenance": summary.runs_kept_by_provenance,
        "errors_cutoff": summary.errors_cutoff,
        "errors_eligible": summary.errors_eligible,
        "errors_deleted": summary.errors_deleted,
        "status": "ok",
    }
    if as_json:
        emit_json(payload)
    else:
        mode = "Would delete" if summary.dry_run else "Deleted"
        typer.secho(
            f"{mode} rows older than the retention windows"
            + (" (dry run; pass --apply to delete)" if summary.dry_run else ""),
            fg=typer.colors.GREEN if summary.dry_run else typer.colors.YELLOW,
        )
        emit_fields(
            {
                "runs cutoff": f"{summary.runs_cutoff.date()} ({runs_older_than_days}d)",
                "runs": (
                    f"{summary.runs_eligible} eligible, {summary.runs_deleted} deleted, "
                    f"{summary.runs_kept_by_provenance} kept (observations reference them)"
                ),
                "errors cutoff": f"{summary.errors_cutoff.date()} ({errors_older_than_days}d)",
                "errors": f"{summary.errors_eligible} eligible, {summary.errors_deleted} deleted",
            },
        )
    raise typer.Exit(ExitCode.OK)


__all__ = ["db_app"]
