"""Validation layers.

The layer boundaries from PLAN section 9:

- **transport** (status, content type, size, JSON decoding) and **schema**
  (envelope shape, array alignment, value types, units, timestamps) live in
  :mod:`adcp.api.open_meteo` / :mod:`adcp.api.schemas` / :mod:`adcp.api.mapping`;
- **domain** rules - ranges, hour alignment, window membership, ordering - live in
  :mod:`adcp.validation.rules` and are applied by :mod:`adcp.validation.validator`;
- **referential** rules - the location is active, the source is one of the three
  allowed values, and rows are written against the database's own ``location_id`` -
  are enforced by :mod:`adcp.pipeline.service` together with the schema's
  constraints.
"""

from __future__ import annotations

from adcp.validation.rules import (
    HOUR_ALIGNMENT_TOLERANCE_S,
    RANGE_RULES,
    RangeRule,
    Rejection,
    RejectionCode,
)
from adcp.validation.validator import ValidatedBatch, validate_series

__all__ = [
    "HOUR_ALIGNMENT_TOLERANCE_S",
    "RANGE_RULES",
    "RangeRule",
    "Rejection",
    "RejectionCode",
    "ValidatedBatch",
    "validate_series",
]
