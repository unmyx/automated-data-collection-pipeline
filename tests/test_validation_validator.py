"""The validator: which rows may be stored, and why the others may not."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest

from adcp.models.observation import ObservationSource
from adcp.pipeline.window import CollectionWindow, plan_scheduled_window
from adcp.validation.rules import RejectionCode
from adcp.validation.validator import ValidatedBatch, validate_series
from tests.support import belgrade, build_series

pytestmark = pytest.mark.unit

NOW = datetime(2026, 10, 1, 14, 37, tzinfo=UTC)

# Storage window: 2026-10-01T08:00 .. 14:00 (exclusive). Accept range: the whole
# of 2026-09-30 and 2026-10-01, because the request works in calendar days.
PLANNED = plan_scheduled_window(now=NOW, watermark=None, lookback_hours=6, overlap_hours=0)
WINDOW = PLANNED.bounds

IN_WINDOW = datetime(2026, 10, 1, 12, 0, tzinfo=UTC)


def hours(
    count: int,
    *,
    end: datetime = datetime(2026, 10, 1, 14, 0, tzinfo=UTC),
) -> list[datetime]:
    """``count`` consecutive hours ending just before ``end``."""
    return [end - timedelta(hours=offset) for offset in range(count, 0, -1)]


def validate(  # noqa: PLR0913 - a test entry point with explicit knobs
    moments: list[datetime],
    *,
    overrides: dict[datetime, dict[str, Any]] | None = None,
    max_ratio: float = 0.25,
    window: CollectionWindow = WINDOW,
    source: ObservationSource = ObservationSource.FORECAST,
    series_source: ObservationSource | None = None,
) -> ValidatedBatch:
    location = belgrade()
    series = build_series(
        location=location,
        source=series_source or source,
        moments=moments,
        observation_overrides=overrides,
    )
    return validate_series(
        series,
        expected_location=location,
        expected_source=source,
        window=window,
        now=NOW,
        max_invalid_row_ratio=max_ratio,
    )


def test_in_window_rows_are_accepted_and_normalised() -> None:
    batch = validate(
        hours(3),
        overrides={IN_WINDOW: {"temperature_2m": Decimal("18.334")}},
    )

    assert batch.rows_received == 3
    assert batch.rows_accepted == 3
    assert batch.rows_rejected == 0
    assert batch.rows_skipped == 0
    assert batch.failed is False
    quantised = next(row for row in batch.accepted if row.observed_at == IN_WINDOW)
    assert quantised.temperature_2m == Decimal("18.33")
    assert quantised.observed_at.tzinfo is UTC


def test_the_forecast_tail_is_skipped_not_rejected() -> None:
    """Hours the provider returns beyond the storage window are not judged."""
    tail = [
        datetime(2026, 10, 1, 15, 0, tzinfo=UTC),
        datetime(2026, 10, 1, 16, 0, tzinfo=UTC),
        # Deliberately invalid values: skipped rows must not be evaluated at all.
    ]
    batch = validate(tail, overrides={tail[0]: {"temperature_2m": Decimal("9999")}})

    assert batch.rows_skipped == 2
    assert batch.rows_rejected == 0
    assert batch.rejection_ratio == 0.0
    assert batch.failed is False


def test_rows_outside_the_requested_range_are_rejected() -> None:
    ancient = datetime(2026, 9, 29, 12, 0, tzinfo=UTC)

    batch = validate([ancient, IN_WINDOW], max_ratio=0.9)

    assert batch.rows_rejected == 1
    assert batch.rejections[0].code is RejectionCode.OUT_OF_WINDOW
    assert batch.rejections[0].fields == ("observed_at",)
    assert batch.rows_accepted == 1


def test_misaligned_timestamps_are_rejected() -> None:
    batch = validate([IN_WINDOW.replace(minute=30)], max_ratio=0.9)

    rejection = batch.rejections[0]
    assert rejection.code is RejectionCode.NOT_HOUR_ALIGNED
    assert rejection.observed_at == IN_WINDOW.replace(minute=30)
    assert rejection.slug == "belgrade-rs"
    assert rejection.source is ObservationSource.FORECAST


def test_timestamps_within_the_tolerance_snap_to_the_hour() -> None:
    almost = IN_WINDOW.replace(minute=59, second=59, microsecond=999_000)

    batch = validate([almost])

    assert batch.rows_accepted == 1
    assert batch.accepted[0].observed_at == IN_WINDOW + timedelta(hours=1)


def test_duplicate_timestamps_are_rejected() -> None:
    batch = validate([IN_WINDOW, IN_WINDOW, IN_WINDOW], max_ratio=0.9)

    assert batch.rows_accepted == 1
    assert batch.rows_rejected == 2
    assert {rejection.code for rejection in batch.rejections} == {RejectionCode.DUPLICATE_TIMESTAMP}


def test_future_rows_inside_the_window_are_rejected() -> None:
    """A window that reaches into the future is a planning bug; rows are refused."""
    future_window = CollectionWindow(
        storage_start=WINDOW.storage_start,
        storage_end=NOW + timedelta(hours=6),
        accept_from=WINDOW.accept_from,
        accept_to=WINDOW.accept_to,
    )
    future_hour = NOW.replace(minute=0, second=0, microsecond=0) + timedelta(hours=3)

    batch = validate([future_hour], window=future_window, max_ratio=0.9)

    assert batch.rejections[0].code is RejectionCode.FUTURE_TIMESTAMP


def test_domain_invalid_values_are_rejected_with_the_field() -> None:
    batch = validate(
        hours(4),
        overrides={
            IN_WINDOW: {
                "temperature_2m": Decimal("842.0"),
                "precipitation": Decimal("-1"),
                "weather_code": 150,
            },
        },
    )

    assert batch.rows_accepted == 3
    assert batch.rows_rejected == 1
    rejection = batch.rejections[0]
    assert rejection.code is RejectionCode.INVALID_WEATHER_CODE
    assert set(rejection.fields) == {"temperature_2m", "precipitation", "weather_code"}
    assert "842.0" in rejection.message
    assert rejection.as_details()["location"] == "belgrade-rs"


def test_every_row_is_accounted_for() -> None:
    tail = [datetime(2026, 10, 1, 18, 0, tzinfo=UTC)]
    ancient = [datetime(2026, 9, 29, 12, 0, tzinfo=UTC)]

    batch = validate([*hours(2), *tail, *ancient])

    assert batch.rows_received == 4
    assert batch.rows_accepted + batch.rows_rejected + batch.rows_skipped == 4
    assert batch.evaluated == 3, "skipped rows are outside the storage window"


def test_location_identity_mismatch_fails_the_payload() -> None:
    other = belgrade()
    series = build_series(
        location=other.__class__(
            slug="reykjavik-is",
            name="Reykjavik",
            latitude=Decimal("64.1466"),
            longitude=Decimal("-21.9426"),
        ),
        source=ObservationSource.FORECAST,
        moments=hours(2),
    )

    batch = validate_series(
        series,
        expected_location=other,
        expected_source=ObservationSource.FORECAST,
        window=WINDOW,
        now=NOW,
        max_invalid_row_ratio=0.25,
    )

    assert batch.failed is True
    assert batch.payload_failure is not None
    assert batch.payload_failure.code is RejectionCode.LOCATION_MISMATCH
    assert batch.rows_accepted == 0


def test_source_identity_mismatch_fails_the_payload() -> None:
    series = build_series(
        location=belgrade(),
        source=ObservationSource.ARCHIVE,
        moments=hours(2),
    )

    batch = validate_series(
        series,
        expected_location=belgrade(),
        expected_source=ObservationSource.FORECAST,
        window=WINDOW,
        now=NOW,
        max_invalid_row_ratio=0.25,
    )

    assert batch.payload_failure is not None
    assert batch.payload_failure.code is RejectionCode.SOURCE_MISMATCH


def test_rejection_budget_withholds_the_whole_payload() -> None:
    bad = {moment: {"temperature_2m": Decimal("500")} for moment in hours(2)}

    batch = validate(hours(4), overrides=bad, max_ratio=0.25)

    assert batch.failed is True
    assert batch.payload_failure is not None
    assert batch.payload_failure.code is RejectionCode.REJECTION_BUDGET_EXCEEDED
    assert batch.rows_accepted == 0, "nothing may be written when the budget trips"
    assert batch.withheld == 2, "the valid rows are withheld, not stored"
    assert batch.rows_rejected == 4
    assert batch.rejection_ratio == 1.0


def test_rejection_budget_within_tolerance_keeps_the_valid_rows() -> None:
    bad = {IN_WINDOW: {"temperature_2m": Decimal("500")}}

    batch = validate(hours(4), overrides=bad, max_ratio=0.5)

    assert batch.failed is False
    assert batch.rows_accepted == 3
    assert batch.rows_rejected == 1
    assert batch.rejection_ratio == pytest.approx(0.25)
    assert batch.withheld == 0


def test_batch_serialises_for_logs() -> None:
    described = validate(hours(2)).as_dict()

    assert described["rows_received"] == 2
    assert described["rows_accepted"] == 2
    assert described["failure"] is None
    assert described["rejection_ratio"] == 0.0
