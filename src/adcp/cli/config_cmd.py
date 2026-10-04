"""``adcp config ...`` - inspect and validate configuration."""

from __future__ import annotations

from typing import Annotated

import typer

from adcp import __version__
from adcp.cli.common import emit_fields, emit_json, load_settings
from adcp.exit_codes import ExitCode

config_app = typer.Typer(
    name="config",
    help="Inspect and validate configuration.",
    no_args_is_help=True,
)


@config_app.command("show")
def config_show(
    as_json: Annotated[bool, typer.Option("--json", help="Emit machine-readable output.")] = False,
) -> None:
    """Print the resolved configuration with secrets masked."""
    settings = load_settings()
    payload = settings.safe_dump()

    if as_json:
        emit_json(payload)
        return
    emit_fields(payload)


@config_app.command("check")
def config_check(
    as_json: Annotated[bool, typer.Option("--json", help="Emit machine-readable output.")] = False,
) -> None:
    """Validate configuration and exit non-zero when it is unusable.

    This performs no network or database I/O; connectivity checks live in
    ``adcp db ping``.
    """
    settings = load_settings()
    summary = {
        "database_url": settings.masked_database_url(),
        "env": settings.env,
        "log_format": settings.log_format,
        "log_level": settings.log_level,
        "status": "ok",
        "version": __version__,
    }
    if as_json:
        emit_json(summary)
    else:
        typer.secho("Configuration OK", fg=typer.colors.GREEN)
        emit_fields(
            {
                "environment": settings.env,
                "database": settings.masked_database_url(),
                "logging": f"{settings.log_level} / {settings.log_format}",
            },
        )
    raise typer.Exit(ExitCode.OK)


__all__ = ["config_app"]
