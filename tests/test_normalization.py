"""Canonical representation and the content hash (PLAN section 10.2)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal

import pytest

from adcp.models.observation import HOURLY_VARIABLES
from adcp.normalization import (
    DECIMAL_PLACES,
    HASH_VERSION,
    canonical_values,
    hash_input,
    normalize_observation,
    normalize_timestamp,
    quantize_decimal,
    row_hash,
)
from tests.support import build_observation

pytestmark = pytest.mark.unit

HOUR = datetime(2026, 10, 1, 12, 0, tzinfo=UTC)


def test_decimal_scale_matches_the_fact_table() -> None:
    assert DECIMAL_PLACES == 2
    assert quantize_decimal(Decimal("18.334")) == Decimal("18.33")
    assert quantize_decimal(Decimal("18.335")) == Decimal("18.34")
    assert quantize_decimal(Decimal("-0.005")) == Decimal("-0.01")
    assert quantize_decimal(Decimal("18.3")) == Decimal("18.30")


def test_timestamp_normalisation_converts_to_utc() -> None:
    berlin = timezone(timedelta(hours=2))

    normalized = normalize_timestamp(datetime(2026, 10, 1, 14, 0, tzinfo=berlin))

    assert normalized == HOUR
    assert normalized.tzinfo is UTC


def test_naive_timestamps_are_rejected() -> None:
    with pytest.raises(ValueError, match="naive"):
        normalize_timestamp(datetime(2026, 10, 1, 12, 0))  # noqa: DTZ001 - the case under test


def test_normalize_observation_quantises_and_localises() -> None:
    berlin = timezone(timedelta(hours=2))
    observation = build_observation(
        datetime(2026, 10, 1, 14, 0, tzinfo=berlin),
        temperature_2m=Decimal("18.334"),
        precipitation=Decimal("0.129"),
    )

    normalized = normalize_observation(observation)

    assert normalized.observed_at == HOUR
    assert normalized.temperature_2m == Decimal("18.33")
    assert normalized.precipitation == Decimal("0.13")


def test_normalize_observation_keeps_integers_and_nulls() -> None:
    observation = build_observation(HOUR, weather_code=3, wind_direction_10m=180)

    normalized = normalize_observation(observation)

    assert normalized.weather_code == 3
    assert normalized.wind_direction_10m == 180
    assert normalized.temperature_2m == Decimal("12.50")
    assert normalized.pressure_msl is None


def test_canonical_values_are_deterministic_and_ordered() -> None:
    first = canonical_values(build_observation(HOUR))
    second = canonical_values(build_observation(HOUR))

    assert list(first) == list(second)
    assert first == second
    assert first["pressure_msl"] == "null"
    assert first["temperature_2m"] == "12.50"


def test_hash_is_stable_for_equivalent_records() -> None:
    equivalent = [
        build_observation(HOUR, temperature_2m=Decimal("18.3")),
        build_observation(HOUR, temperature_2m=Decimal("18.30")),
        build_observation(HOUR, temperature_2m=Decimal("18.300")),
    ]

    hashes = {row_hash(observation) for observation in equivalent}

    assert len(hashes) == 1, "equivalent normalised records must hash identically"


def test_hash_changes_when_source_data_changes() -> None:
    base = build_observation(HOUR, temperature_2m=Decimal("18.30"))

    changed = build_observation(HOUR, temperature_2m=Decimal("18.31"))
    nulled = build_observation(HOUR, temperature_2m=None)

    assert row_hash(base) != row_hash(changed)
    assert row_hash(base) != row_hash(nulled), "a missing measurement is not a zero"


def test_hash_excludes_the_timestamp_because_the_key_covers_it() -> None:
    """The natural key is (location, observed_at, source); the hash is content only."""
    base = build_observation(HOUR, temperature_2m=Decimal("18.30"))
    later = build_observation(HOUR + timedelta(hours=1), temperature_2m=Decimal("18.30"))

    assert row_hash(base) == row_hash(later)
    assert "observed_at" not in canonical_values(base)


def test_hash_is_insensitive_to_provenance() -> None:
    """Only measurements are hashed; when we looked must not change the hash."""
    observation = build_observation(HOUR, temperature_2m=Decimal("18.30"))

    assert row_hash(observation) == row_hash(observation)
    assert "run_id" not in canonical_values(observation)
    assert "row_hash" not in canonical_values(observation)
    assert set(canonical_values(observation)) == set(HOURLY_VARIABLES)


def test_hash_input_is_versioned_and_serialised_deterministically() -> None:
    observation = build_observation(HOUR, temperature_2m=Decimal("18.30"))

    payload = hash_input(observation)

    assert payload.hash_version == HASH_VERSION
    assert payload.serialized() == hash_input(observation).serialized()
    assert '"hash_version":1' in payload.serialized().replace(" ", "")


def test_row_hash_is_a_sha256_hex_digest() -> None:
    digest = row_hash(build_observation(HOUR))

    assert len(digest) == 64
    assert set(digest) <= set("0123456789abcdef")
