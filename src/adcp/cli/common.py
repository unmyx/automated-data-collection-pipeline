"""Helpers shared by the CLI command modules.

Kept separate from :mod:`adcp.cli.main` so command modules can import them
without an import cycle (``main`` registers the sub-applications).
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

import typer
from pydantic import ValidationError

from adcp.config import Settings, get_settings
from adcp.exit_codes import ExitCode


def format_settings_errors(exc: ValidationError) -> list[str]:
    """Render pydantic validation errors as ``field: message`` lines."""
    lines: list[str] = []
    for error in exc.errors():
        location = ".".join(str(part) for part in error.get("loc", ())) or "<root>"
        message = str(error.get("msg", "invalid value"))
        lines.append(f"{location}: {message}")
    return lines or ["<root>: configuration is invalid"]


def load_settings() -> Settings:
    """Load settings, reporting every validation problem and exiting 2 on failure."""
    try:
        return get_settings()
    except ValidationError as exc:
        typer.secho("Configuration is invalid:", fg=typer.colors.RED, err=True)
        for line in format_settings_errors(exc):
            typer.secho(f"  - {line}", err=True)
        typer.secho(
            "Fix the ADCP_* environment variables (see .env.example) and try again.",
            err=True,
        )
        raise typer.Exit(ExitCode.CONFIG_ERROR) from exc


def render_value(value: Any) -> str:
    """Render one configuration/result value for human output."""
    if value is None:
        return "(unset)"
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def emit_fields(fields: Mapping[str, Any]) -> None:
    """Print an aligned ``name  value`` block, preserving insertion order."""
    if not fields:
        return
    width = max(len(key) for key in fields)
    for key, value in fields.items():
        typer.echo(f"{key:<{width}}  {render_value(value)}")


def emit_json(payload: Any) -> None:
    """Print machine-readable output."""
    typer.echo(json.dumps(payload, indent=2, sort_keys=True, default=str))


def emit_error(message: str) -> None:
    """Print a failure to stderr. Never raise a traceback for expected failures."""
    typer.secho(f"Error: {message}", fg=typer.colors.RED, err=True)


__all__ = [
    "emit_error",
    "emit_fields",
    "emit_json",
    "format_settings_errors",
    "load_settings",
    "render_value",
]
