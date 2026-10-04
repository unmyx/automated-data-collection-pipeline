"""Process exit codes.

These values are a public contract: schedulers, cron jobs, and CI pipelines branch
on them (see ``docs/PLAN.md`` sections 11.4 and 13.4).
"""

from __future__ import annotations

from enum import IntEnum


class ExitCode(IntEnum):
    """Exit codes returned by the ``adcp`` CLI."""

    OK = 0
    """Success, or nothing to do (lock contention, no active locations)."""

    FAILURE = 1
    """Hard failure: the run did not produce usable output."""

    CONFIG_ERROR = 2
    """Configuration or usage error: nothing was read from or written to upstream."""

    PARTIAL = 3
    """Partial success: some data was written, some units of work failed."""


__all__ = ["ExitCode"]
