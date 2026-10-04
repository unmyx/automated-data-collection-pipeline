"""Window planning: storage bounds, accept range, and truncation."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from adcp.pipeline.window import (
    CollectionWindow,
    RowPlacement,
    floor_to_hour,
    plan_scheduled_window,
    start_of_day,
)

pytestmark = pytest.mark.unit

#: A Thursday, mid-afternoon: the current day is partially complete.
NOW = datetime(2026, 10, 1, 14, 37, tzinfo=UTC)


def test_flooring_and_day_bounds() -> None:
    assert floor_to_hour(NOW) == datetime(2026, 10, 1, 14, 0, tzinfo=UTC)
    assert start_of_day(NOW) == datetime(2026, 10, 1, 0, 0, tzinfo=UTC)


def test_first_run_uses_the_whole_lookback() -> None:
    plan = plan_scheduled_window(now=NOW, watermark=None, lookback_hours=72, overlap_hours=24)

    assert plan.request.past_days == 3
    assert plan.request.forecast_days == 1
    assert plan.bounds.storage_end == datetime(2026, 10, 1, 14, 0, tzinfo=UTC)
    assert plan.bounds.storage_start == datetime(2026, 9, 28, 14, 0, tzinfo=UTC)
    assert plan.bounds.hours == 72
    assert plan.truncated is False


def test_watermark_minus_overlap_is_the_floor() -> None:
    watermark = datetime(2026, 10, 1, 9, 0, tzinfo=UTC)

    plan = plan_scheduled_window(now=NOW, watermark=watermark, lookback_hours=72, overlap_hours=24)

    assert plan.bounds.storage_start == datetime(2026, 9, 30, 9, 0, tzinfo=UTC)
    assert plan.truncated is False


def test_a_long_gap_is_truncated_to_the_lookback() -> None:
    watermark = datetime(2026, 9, 1, 0, 0, tzinfo=UTC)

    plan = plan_scheduled_window(now=NOW, watermark=watermark, lookback_hours=72, overlap_hours=24)

    assert plan.bounds.storage_start == datetime(2026, 9, 28, 14, 0, tzinfo=UTC)
    assert plan.truncated is True


def test_accept_range_covers_whole_calendar_days() -> None:
    """Open-Meteo selects whole days, so the payload reaches past the storage window."""
    plan = plan_scheduled_window(now=NOW, watermark=None, lookback_hours=48, overlap_hours=24)

    assert plan.request.past_days == 2
    assert plan.bounds.accept_from == datetime(2026, 9, 29, 0, 0, tzinfo=UTC)
    assert plan.bounds.accept_to == datetime(2026, 10, 2, 0, 0, tzinfo=UTC)


@pytest.mark.parametrize(
    ("offset_hours", "expected"),
    [
        (-1, RowPlacement.STORED),
        (-6, RowPlacement.STORED),
        (1, RowPlacement.SKIPPED),  # the forecast tail of today
        (9, RowPlacement.SKIPPED),
        (11, RowPlacement.OUT_OF_RANGE),  # past the end of today
        (-80, RowPlacement.SKIPPED),  # the over-fetched start of the first day
        (-100, RowPlacement.OUT_OF_RANGE),  # before the days we asked for
    ],
)
def test_placement_classification(offset_hours: int, expected: RowPlacement) -> None:
    plan = plan_scheduled_window(now=NOW, watermark=None, lookback_hours=72, overlap_hours=24)
    moment = plan.bounds.storage_end + timedelta(hours=offset_hours)

    assert plan.bounds.placement(moment) is expected
    assert plan.bounds.contains(moment) is (expected is RowPlacement.STORED)


def test_empty_windows_are_rejected() -> None:
    with pytest.raises(ValueError, match="empty"):
        CollectionWindow(
            storage_start=NOW,
            storage_end=NOW,
            accept_from=NOW - timedelta(days=1),
            accept_to=NOW,
        )


def test_window_serialises_for_logs_and_json() -> None:
    plan = plan_scheduled_window(now=NOW, watermark=None, lookback_hours=24, overlap_hours=0)

    described = plan.bounds.as_dict()

    assert described["storage_hours"] == 24
    assert described["truncated"] is False
    assert str(described["storage_start"]).startswith("2026-09-30T14:00")
