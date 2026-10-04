"""CLI application assembly.

Importing this package wires every command group onto the root Typer app, so
``from adcp.cli import app`` (tests, ``python -m adcp``, and the console script)
always sees the complete command tree.
"""

from __future__ import annotations

from adcp.cli import collect_cmd, config_cmd, db_cmd, schedule_cmd
from adcp.cli.main import INTERRUPTED_EXIT_CODE, app, entrypoint, exit_code_for_error, load_settings

app.add_typer(config_cmd.config_app, name="config")
app.add_typer(db_cmd.db_app, name="db")
app.command("collect")(collect_cmd.collect)
app.command("schedule")(schedule_cmd.schedule)

__all__ = [
    "INTERRUPTED_EXIT_CODE",
    "app",
    "entrypoint",
    "exit_code_for_error",
    "load_settings",
]
