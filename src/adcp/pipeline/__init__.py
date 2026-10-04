"""Collection pipeline: window planning, validation, persistence orchestration.

The pipeline layer is where the adapters meet: it reads locations and watermarks
from PostgreSQL, calls the :class:`~adcp.ports.WeatherSource` adapter, validates
what came back, and writes observations back to PostgreSQL inside one transaction
per location (PLAN sections 4.1 and 11.1).
"""

from __future__ import annotations

from adcp.pipeline.window import (
    CollectionWindow,
    RowPlacement,
    ScheduledWindow,
    plan_scheduled_window,
)

__all__ = [
    "CollectionWindow",
    "RowPlacement",
    "ScheduledWindow",
    "plan_scheduled_window",
]
