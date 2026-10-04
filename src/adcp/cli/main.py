"""Root CLI application: ``adcp`` itself plus the ``version`` command."""

from __future__ import annotations

import json
from typing import Annotated, NoReturn

import typer
from pydantic import ValidationError

from adcp import __version__
from adcp.cli.common import load_settings
from adcp.config import get_settings
from adcp.errors import AdcpError, ConfigurationError
from adcp.exit_codes import ExitCode
from adcp.logging import configure_logging, get_logger, mask_credentials_in_text

_logger = get_logger(__name__)

#: Shell convention for a process stopped by Ctrl+C.
INTERRUPTED_EXIT_CODE = 130

app = typer.Typer(
    name="adcp",
    help="Automated Data Collection Pipeline - collect hourly weather data into PostgreSQL.",
    no_args_is_help=True,
    add_completion=False,
    rich_markup_mode=None,
)


def _print_version_callback(value: bool) -> None:
    """Eager ``--version`` handler: print and exit before any command runs."""
    if value:
        typer.echo(f"adcp {__version__}")
        raise typer.Exit(ExitCode.OK)


@app.callback()
def _root(
    version: Annotated[  # noqa: ARG001 - value is consumed by the eager --version callback
        bool,
        typer.Option(
            "--version",
            "-V",
            callback=_print_version_callback,
            is_eager=True,
            help="Show the application version and exit.",
        ),
    ] = False,
) -> None:
    """Configure process-wide logging before dispatching to a command."""
    try:
        settings = get_settings()
    except ValidationError:
        # Commands report configuration problems themselves, with full detail.
        return
    configure_logging(
        level=settings.log_level,
        log_format=settings.log_format,
        service=settings.service_name,
        environment=settings.env,
        include_caller=settings.log_include_caller,
    )


@app.command("version")
def version_command(
    as_json: Annotated[bool, typer.Option("--json", help="Emit machine-readable output.")] = False,
) -> None:
    """Print the application version."""
    if as_json:
        typer.echo(json.dumps({"name": "adcp", "version": __version__}, sort_keys=True))
    else:
        typer.echo(f"adcp {__version__}")


def exit_code_for_error(error: AdcpError) -> ExitCode:
    """Map an error that reached the top of the CLI onto the documented codes."""
    if isinstance(error, ConfigurationError):
        return ExitCode.CONFIG_ERROR
    return ExitCode.FAILURE


def entrypoint() -> NoReturn:
    """Console-script entry point (``adcp``).

    Commands handle their own expected failures, so this is the last line of
    defence: nothing reaching it may dump a traceback on an operator. Mapped
    errors keep their documented exit code, unexpected ones are logged in full and
    reported as a one-line bug report with exit code 1.
    """
    try:
        app(prog_name="adcp")
    except AdcpError as error:
        typer.secho(
            f"Error: {mask_credentials_in_text(str(error))}",
            fg=typer.colors.RED,
            err=True,
        )
        raise SystemExit(exit_code_for_error(error)) from error
    except KeyboardInterrupt:
        typer.secho("Interrupted", err=True)
        raise SystemExit(INTERRUPTED_EXIT_CODE) from None
    except Exception as error:
        # The CLI must never leak a traceback; structlog's API carries the stack.
        _logger.error(  # noqa: G201
            "cli.unexpected_error",
            error_type=type(error).__name__,
            message=mask_credentials_in_text(str(error)),
            exc_info=True,
        )
        typer.secho(
            f"Unexpected error ({type(error).__name__}): {mask_credentials_in_text(str(error))}",
            fg=typer.colors.RED,
            err=True,
        )
        typer.secho(
            "Set ADCP_LOG_LEVEL=DEBUG and check the structured logs; this looks like a bug.",
            err=True,
        )
        raise SystemExit(ExitCode.FAILURE) from error
    raise SystemExit(ExitCode.OK)


__all__ = ["INTERRUPTED_EXIT_CODE", "app", "entrypoint", "exit_code_for_error", "load_settings"]
