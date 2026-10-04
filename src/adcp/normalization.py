"""Canonical representation of an observation, and the content hash.

The fact table's natural key is ``(location_id, observed_at, source)``; the
content hash decides whether a re-observation actually carries new information
(PLAN sections 10.2 and 10.3). Two rules make that work:

1. **Quantise before hashing.** ``weather_hourly`` stores ``numeric`` columns with
   two decimal places, so a value of ``18.334`` is stored as ``18.33``. Hashing
   the unquantised value would report a "revision" for a row whose stored content
   never changed.
2. **Exclude provenance.** ``run_id``, ``collected_at`` and ``revision_count``
   describe when we looked, not what we saw, so they must not change the hash.

``HASH_VERSION`` is part of the canonical payload: changing the formula or the
column set deliberately invalidates every stored hash, and that change is
observable in the run summary rather than silent.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from decimal import ROUND_HALF_UP, Decimal
from typing import Any, Final

from adcp.models.observation import HOURLY_VARIABLES, WeatherObservation

#: Bump when the hash input changes so the rewrite is deliberate.
HASH_VERSION: Final[int] = 1

#: Scale of every measurement column in ``weather_hourly`` (``numeric(_, 2)``).
DECIMAL_PLACES: Final[int] = 2
_QUANTUM: Final[Decimal] = Decimal(1).scaleb(-DECIMAL_PLACES)

#: Variables stored as ``smallint`` (PLAN section 5.3) rather than ``numeric``.
INTEGER_VARIABLES: Final[frozenset[str]] = frozenset({"weather_code", "wind_direction_10m"})


def quantize_decimal(value: Decimal) -> Decimal:
    """Round a measurement to the storage scale, half away from zero."""
    return value.quantize(_QUANTUM, rounding=ROUND_HALF_UP)


def normalize_timestamp(moment: datetime) -> datetime:
    """Return the moment as an aware UTC datetime."""
    if moment.tzinfo is None or moment.utcoffset() is None:
        msg = f"timestamp {moment!r} is naive; observations are always stored in UTC"
        raise ValueError(msg)
    return moment.astimezone(UTC)


def normalize_observation(observation: WeatherObservation) -> WeatherObservation:
    """Return the canonical form of an observation: UTC and storage-scale values."""
    values: dict[str, Any] = {}
    for name in HOURLY_VARIABLES:
        value = getattr(observation, name)
        if value is None or name in INTEGER_VARIABLES:
            values[name] = value
        else:
            values[name] = quantize_decimal(Decimal(value))
    return replace(
        observation,
        observed_at=normalize_timestamp(observation.observed_at),
        **values,
    )


def canonical_values(observation: WeatherObservation) -> dict[str, str]:
    """Deterministic string form of every measurement, ready to be hashed.

    ``None`` becomes the literal ``"null"`` so a missing measurement is distinct
    from a measured zero. Integers keep their integer form, decimals keep exactly
    the stored scale.
    """
    canonical: dict[str, str] = {}
    for name in HOURLY_VARIABLES:
        value = getattr(observation, name)
        if value is None:
            canonical[name] = "null"
        elif name in INTEGER_VARIABLES:
            canonical[name] = str(int(value))
        else:
            canonical[name] = str(quantize_decimal(Decimal(value)))
    return canonical


@dataclass(frozen=True, slots=True)
class HashInput:
    """The hashed payload, useful in tests and when explaining a revision."""

    hash_version: int
    values: dict[str, str]

    def serialized(self) -> str:
        return json.dumps(
            {"hash_version": self.hash_version, "values": self.values},
            sort_keys=True,
            separators=(",", ":"),
        )


def hash_input(observation: WeatherObservation) -> HashInput:
    """Build the canonical hash payload for an observation."""
    return HashInput(hash_version=HASH_VERSION, values=canonical_values(observation))


def row_hash(observation: WeatherObservation) -> str:
    """sha256 of the canonical payload: stable, order-independent, versioned."""
    return hashlib.sha256(hash_input(observation).serialized().encode("utf-8")).hexdigest()


__all__ = [
    "DECIMAL_PLACES",
    "HASH_VERSION",
    "INTEGER_VARIABLES",
    "HashInput",
    "canonical_values",
    "hash_input",
    "normalize_observation",
    "normalize_timestamp",
    "quantize_decimal",
    "row_hash",
]
