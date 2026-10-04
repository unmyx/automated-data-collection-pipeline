"""Run-level accounting: counts aggregation and terminal status."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from adcp.models.observation import ObservationSource
from adcp.models.run import (
    LocationResult,
    LocationStatus,
    RequestStats,
    RunCounts,
    RunStatus,
    RunSummary,
)
from adcp.pipeline.service import compute_run_status

pytestmark = pytest.mark.unit

NOW = datetime(2026, 10, 1, 14, 0, tzinfo=UTC)


def result(  # noqa: PLR0913 - a compact builder for the many counter fields
    slug: str,
    *,
    status: LocationStatus = LocationStatus.SUCCEEDED,
    inserted: int = 0,
    updated: int = 0,
    unchanged: int = 0,
    rejected: int = 0,
) -> LocationResult:
    return LocationResult(
        slug=slug,
        source=ObservationSource.FORECAST,
        status=status,
        rows_received=inserted + updated + unchanged + rejected,
        rows_accepted=inserted + updated + unchanged,
        rows_inserted=inserted,
        rows_updated=updated,
        rows_unchanged=unchanged,
        rows_rejected=rejected,
    )


def test_request_stats_delta() -> None:
    before = RequestStats(requests_made=2, requests_retried=1)
    after = RequestStats(requests_made=7, requests_retried=3)

    assert before.delta(after) == RequestStats(requests_made=5, requests_retried=2)
    assert after.delta(before) == RequestStats(), "counters never go backwards"


def test_counts_aggregate_across_locations() -> None:
    counts = RunCounts.from_results(
        [
            result("a", inserted=3, unchanged=1),
            result("b", status=LocationStatus.FAILED, updated=2, rejected=4),
        ],
        locations_total=3,
        requests_made=2,
        requests_retried=1,
    )

    assert counts.locations_total == 3
    assert counts.locations_succeeded == 1
    assert counts.locations_failed == 1
    assert counts.rows_received == 10
    assert counts.rows_accepted == 6
    assert counts.rows_rejected == 4
    assert counts.requests_made == 2
    assert counts.requests_retried == 1
    assert counts.error_count == 1


def test_counts_db_columns_exclude_derived_values() -> None:
    columns = RunCounts(rows_accepted=9, rows_skipped=4).to_db_columns()

    assert "rows_accepted" not in columns
    assert "rows_skipped" not in columns
    assert set(columns) == {
        "locations_total",
        "locations_succeeded",
        "locations_failed",
        "requests_made",
        "requests_retried",
        "rows_received",
        "rows_inserted",
        "rows_updated",
        "rows_unchanged",
        "rows_rejected",
        "error_count",
    }


def test_status_succeeded_when_everything_works() -> None:
    status, exceeded, summary = compute_run_status(
        [result("a", inserted=1), result("b", inserted=1)],
        locations_requested=2,
        failure_budget_ratio=0.5,
    )

    assert status is RunStatus.SUCCEEDED
    assert exceeded is False
    assert summary is None


def test_status_partial_when_a_location_fails_within_budget() -> None:
    status, exceeded, summary = compute_run_status(
        [result("a", inserted=1), result("b", status=LocationStatus.FAILED)],
        locations_requested=4,
        failure_budget_ratio=0.5,
    )

    assert status is RunStatus.PARTIAL
    assert exceeded is False
    assert summary is not None
    assert "failed" in summary


def test_status_partial_when_rows_are_rejected() -> None:
    status, exceeded, _ = compute_run_status(
        [result("a", inserted=3, rejected=1)],
        locations_requested=1,
        failure_budget_ratio=0.5,
    )

    assert status is RunStatus.PARTIAL
    assert exceeded is False


def test_status_failed_when_every_location_fails() -> None:
    status, exceeded, summary = compute_run_status(
        [result("a", status=LocationStatus.FAILED), result("b", status=LocationStatus.FAILED)],
        locations_requested=2,
        failure_budget_ratio=1.0,
    )

    assert status is RunStatus.FAILED
    assert exceeded is False, "the budget was not the reason"
    assert summary is not None
    assert "every location failed" in summary


def test_failure_budget_trips_the_run() -> None:
    status, exceeded, summary = compute_run_status(
        [
            result("a", status=LocationStatus.FAILED),
            result("b", status=LocationStatus.FAILED),
            result("c", inserted=1),
        ],
        locations_requested=3,
        failure_budget_ratio=0.5,
    )

    assert status is RunStatus.FAILED
    assert exceeded is True
    assert summary is not None
    assert "budget" in summary


def test_status_skipped_without_locations_and_failed_without_results() -> None:
    skipped, _, _ = compute_run_status([], locations_requested=0, failure_budget_ratio=0.5)
    failed, _, _ = compute_run_status([], locations_requested=3, failure_budget_ratio=0.5)

    assert skipped is RunStatus.SKIPPED
    assert failed is RunStatus.FAILED


def test_summary_payload_is_json_ready() -> None:
    summary = RunSummary(
        status=RunStatus.PARTIAL,
        trigger="cli",
        started_at=NOW,
        finished_at=NOW,
        counts=RunCounts.from_results([result("a", inserted=1)], locations_total=1),
        run_id=None,
        results=(result("a", inserted=1),),
    )

    payload = summary.as_dict()

    assert payload["status"] == "partial"
    assert payload["run_id"] is None
    assert payload["rows_inserted"] == 1
    assert payload["locations_attempted"] == 1
    assert payload["duration_ms"] == 0
