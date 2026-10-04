"""Run accounting: terminal state, counters, and quarantined errors."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

import pytest
import sqlalchemy as sa
from sqlalchemy.engine import Engine

from adcp.db.engine import connection_scope
from adcp.db.run_tracker import (
    MAX_ERROR_MESSAGE_CHARS,
    MAX_PAYLOAD_SAMPLE_CHARS,
    RunTracker,
)
from adcp.db.tables import ingestion_run_errors
from adcp.errors import RunNotFoundError
from adcp.models.run import RunStatus

pytestmark = pytest.mark.integration


def start(engine: Engine) -> uuid.UUID:
    return (
        RunTracker(engine)
        .start_run(
            run_type="manual",
            trigger="test",
            app_version="0.0.0",
        )
        .id
    )


def errors_for(engine: Engine, run_id: uuid.UUID) -> list[dict[str, object]]:
    statement = (
        sa.select(ingestion_run_errors)
        .where(ingestion_run_errors.c.run_id == run_id)
        .order_by(ingestion_run_errors.c.id)
    )
    with engine.connect() as connection:
        return [dict(row) for row in connection.execute(statement).mappings()]


def test_finish_run_records_the_terminal_state(db_engine: Engine) -> None:
    tracker = RunTracker(db_engine)
    run_id = start(db_engine)
    # Must be after the run started, or the table's CHECK constraint fires.
    finished = datetime.now(UTC)

    with connection_scope(db_engine) as connection:
        record = tracker.finish_run(
            connection,
            run_id,
            status=RunStatus.PARTIAL,
            counts={
                "locations_total": 3,
                "locations_succeeded": 2,
                "locations_failed": 1,
                "rows_received": 100,
                "rows_inserted": 40,
                "rows_updated": 5,
                "rows_unchanged": 50,
                "rows_rejected": 5,
                "error_count": 1,
                "requests_made": 3,
                "requests_retried": 1,
            },
            error_summary="one location failed",
            duration_ms=1_234,
            finished_at=finished,
        )

    assert record.status == "partial"
    assert record.finished_at == finished
    assert record.locations_total == 3
    assert record.rows_inserted == 40
    assert record.rows_rejected == 5
    assert record.error_count == 1
    assert record.error_summary == "one location failed"


def test_finish_run_rejects_unknown_counter_columns(db_engine: Engine) -> None:
    run_id = start(db_engine)

    with (
        pytest.raises(ValueError, match="not an ingestion_runs counter column"),
        connection_scope(db_engine) as connection,
    ):
        RunTracker(db_engine).finish_run(
            connection,
            run_id,
            status=RunStatus.SUCCEEDED,
            counts={"rows_exploded": 3},
        )


def test_finish_run_on_a_missing_run_is_reported(db_engine: Engine) -> None:
    with pytest.raises(RunNotFoundError), connection_scope(db_engine) as connection:
        RunTracker(db_engine).finish_run(
            connection,
            uuid.uuid4(),
            status=RunStatus.SUCCEEDED,
        )


def test_record_error_keeps_the_relevant_context(db_engine: Engine) -> None:
    run_id = start(db_engine)
    tracker = RunTracker(db_engine)

    with connection_scope(db_engine) as connection:
        tracker.record_error(
            connection,
            run_id,
            phase="validate",
            error_type="OutOfRange",
            message="temperature_2m=842.0 is above the maximum 60",
            attempt=2,
            http_status=200,
            error_code="OutOfRange",
            request_url="https://api.open-meteo.com/v1/forecast?latitude=44.8125",
            payload_sample={"code": "OutOfRange", "fields": ["temperature_2m"]},
        )

    stored = errors_for(db_engine, run_id)
    assert len(stored) == 1
    assert stored[0]["phase"] == "validate"
    assert stored[0]["error_type"] == "OutOfRange"
    assert stored[0]["attempt"] == 2
    assert stored[0]["http_status"] == 200
    assert stored[0]["payload_sample"] == {
        "code": "OutOfRange",
        "fields": ["temperature_2m"],
    }
    assert tracker.count_errors(run_id) == 1


def test_record_error_strips_credentials_and_bounds_payloads(db_engine: Engine) -> None:
    run_id = start(db_engine)
    tracker = RunTracker(db_engine)
    huge = {"blob": "x" * (MAX_PAYLOAD_SAMPLE_CHARS * 2)}
    long_message = "boom " * MAX_ERROR_MESSAGE_CHARS

    with connection_scope(db_engine) as connection:
        tracker.record_error(
            connection,
            run_id,
            phase="fetch",
            error_type="UpstreamServerError",
            message=long_message,
            request_url="https://api.open-meteo.com/v1/forecast?apikey=super-secret&latitude=1",
            payload_sample=huge,
        )

    stored = errors_for(db_engine, run_id)[0]
    message = str(stored["message"])
    request_url = str(stored["request_url"])
    sample = stored["payload_sample"]
    assert "super-secret" not in request_url
    assert "apikey=***" in request_url
    assert len(message) <= MAX_ERROR_MESSAGE_CHARS
    assert isinstance(sample, dict)
    assert sample["truncated"] is True
    assert len(str(sample["excerpt"])) <= MAX_PAYLOAD_SAMPLE_CHARS


def test_record_error_keeps_small_samples_verbatim(db_engine: Engine) -> None:
    run_id = start(db_engine)
    sample = {"reason": "Latitude must be in range", "status": 400}

    with connection_scope(db_engine) as connection:
        RunTracker(db_engine).record_error(
            connection,
            run_id,
            phase="fetch",
            error_type="UpstreamClientError",
            message="rejected",
            payload_sample=sample,
        )

    assert errors_for(db_engine, run_id)[0]["payload_sample"] == sample
