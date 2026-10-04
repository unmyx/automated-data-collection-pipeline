"""ADCP - Automated Data Collection Pipeline.

Scheduled, idempotent ingestion of hourly weather data from the public
Open-Meteo REST API into PostgreSQL.

See ``docs/PLAN.md`` for the design and the milestone plan.
"""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("adcp")
except PackageNotFoundError:  # pragma: no cover - running from a source tree
    __version__ = "0.0.0.dev0"

__all__ = ["__version__"]
