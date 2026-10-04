"""Run-state hygiene: the stale-run reaper and retention pruning."""

from __future__ import annotations

import io
import json
import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
import sqlalchemy as sa
from sqlalchemy.engine import Engine
from typer.testing import CliRunner

from adcp.cli import app
from adcp.config import Settings
from adcp.db.repository import LocationRepository
from adcp.db.run_tracker import REAPED_SUMMARY, RunTracker
from adcp.db.tables import ingestion_run_errors, ingestion_runs, weather_hourly
from adcp.exit_codes import ExitCode
from adcp.logging import configure_logging
from adcp.pipeline.service import CollectionService
from tests.support import FakeWeatherSource, build_series, events_named

pytestmark = pytest.mark.integration

runner = CliRunner()
NOW = datetime.now(UTC)


def insert_run(
    engine: Engine,
    *,
    started_at: datetime,
    status: str = "running",
    finished_at: datetime | None = None,
) -> uuid.UUID:
    statement = (
        ingestion_runs.insert()
        .values(
            run_type="manual",
            trigger="test",
            app_version="0.0.0",
            status=status,
            started_at=started_at,
            finished_at=finished_at,
        )
        .returning(ingestion_runs.c.id)
    )
    with engine.begin() as connection:
        return uuid.UUID(str(connection.execute(statement).scalar_one()))


def insert_error(engine: Engine, run_id: uuid.UUID, *, occurred_at: datetime) -> None:
    with engine.begin() as connection:
        connection.execute(
            ingestion_run_errors.insert().values(
                run_id=run_id,
                phase="fetch",
                error_type="ReadTimeout",
                message="upstream timed out",
                occurred_at=occurred_at,
            ),
        )


def insert_observation(engine: Engine, *, run_id: uuid.UUID, observed_at: datetime) -> None:
    location = LocationRepository(engine).create(
        slug=f"provenance-{uuid.uuid4().hex[:6]}",
        name="Provenance",
        latitude=Decimal("44.812500"),
        longitude=Decimal("20.437500"),
    )
    with engine.begin() as connection:
        connection.execute(
            weather_hourly.insert().values(
                location_id=location.id,
                observed_at=observed_at,
                source="forecast",
                temperature_2m=Decimal("12.50"),
                row_hash="hash-provenance",
                upstream_latitude=Decimal("44.812500"),
                upstream_longitude=Decimal("20.437500"),
                first_seen_run_id=run_id,
                last_seen_run_id=run_id,
            ),
        )


def run_status(engine: Engine, run_id: uuid.UUID) -> tuple[str, str | None]:
    statement = sa.select(ingestion_runs.c.status, ingestion_runs.c.error_summary).where(
        ingestion_runs.c.id == run_id,
    )
    with engine.connect() as connection:
        row = connection.execute(statement).one()
    return str(row[0]), None if row[1] is None else str(row[1])


def test_reaper_fails_runs_left_running_and_touches_nothing_else(db_engine: Engine) -> None:
    stale = insert_run(db_engine, started_at=NOW - timedelta(hours=5))
    recent = insert_run(db_engine, started_at=NOW - timedelta(minutes=1))
    finished = insert_run(
        db_engine,
        started_at=NOW - timedelta(hours=6),
        status="succeeded",
        finished_at=NOW - timedelta(hours=6) + timedelta(minutes=1),
    )

    reaped = RunTracker(db_engine).reap_stale_runs(max_age_s=3_600)

    assert reaped == [stale]
    assert run_status(db_engine, stale) == ("failed", REAPED_SUMMARY)
    assert run_status(db_engine, recent)[0] == "running"
    assert run_status(db_engine, finished)[0] == "succeeded"


def test_reaper_is_a_no_op_without_stale_runs(db_engine: Engine) -> None:
    insert_run(db_engine, started_at=NOW - timedelta(minutes=5))

    assert RunTracker(db_engine).reap_stale_runs(max_age_s=3_600) == []


def test_a_collection_reaps_the_previous_crashed_run(db_engine: Engine) -> None:
    """A killed process must not leave a permanently `running` row behind."""
    buffer = io.StringIO()
    configure_logging(
        level="DEBUG", log_format="json", service="adcp", environment="test", stream=buffer
    )
    stale = insert_run(db_engine, started_at=datetime.now(UTC) - timedelta(hours=4))
    record = LocationRepository(db_engine).create(
        slug="belgrade-rs",
        name="Belgrade",
        latitude=Decimal("44.812500"),
        longitude=Decimal("20.437500"),
    )
    settings = Settings(
        _env_file=None,
        database_url=str(db_engine.url.render_as_string(hide_password=False)),
        ingest_lookback_hours=2,
        ingest_overlap_hours=0,
        run_timeout_s=60,
        open_meteo_max_concurrency=1,
    )
    end = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    moments = [end - timedelta(hours=2), end - timedelta(hours=1)]
    source = FakeWeatherSource(
        responder=lambda location, obs_source, _window: build_series(
            location=location,
            source=obs_source,
            moments=moments,
        ),
    )
    service = CollectionService(settings, source=source, engine=db_engine, clock=lambda: 0.0)

    summary = service.run(trigger="cli")

    assert summary.status.value == "succeeded"
    assert record.id is not None
    assert run_status(db_engine, stale)[0] == "failed"
    reaped = events_named(buffer, "ingest.runs.reaped")
    assert reaped
    assert reaped[0]["count"] == 1
    assert reaped[0]["run_ids"] == [str(stale)]


def test_prune_dry_run_reports_without_deleting(db_engine: Engine) -> None:
    old_run = insert_run(
        db_engine,
        started_at=NOW - timedelta(days=400),
        status="succeeded",
        finished_at=NOW - timedelta(days=400) + timedelta(minutes=1),
    )
    insert_error(db_engine, old_run, occurred_at=NOW - timedelta(days=200))
    tracker = RunTracker(db_engine)

    summary = tracker.prune(runs_older_than_days=365, errors_older_than_days=90, dry_run=True)

    assert summary.dry_run is True
    assert summary.runs_eligible == 1
    assert summary.runs_deleted == 0
    assert summary.errors_eligible == 1
    assert summary.errors_deleted == 0
    assert tracker.count_errors(old_run) == 1
    with db_engine.connect() as connection:
        assert connection.execute(sa.text("SELECT count(*) FROM ingestion_runs")).scalar_one() == 1


def test_prune_deletes_old_errors_and_unreferenced_runs(db_engine: Engine) -> None:
    old_run = insert_run(
        db_engine,
        started_at=NOW - timedelta(days=400),
        status="succeeded",
        finished_at=NOW - timedelta(days=400) + timedelta(minutes=1),
    )
    recent_run = insert_run(
        db_engine,
        started_at=NOW - timedelta(days=2),
        status="partial",
        finished_at=NOW - timedelta(days=2) + timedelta(minutes=1),
    )
    insert_error(db_engine, old_run, occurred_at=NOW - timedelta(days=200))
    insert_error(db_engine, recent_run, occurred_at=NOW - timedelta(days=200))
    tracker = RunTracker(db_engine)

    summary = tracker.prune(runs_older_than_days=365, errors_older_than_days=90, dry_run=False)

    assert summary.runs_deleted == 1
    assert summary.errors_deleted == 2
    assert tracker.count_errors(old_run) == 0
    assert tracker.count_errors(recent_run) == 0, "aged-out errors go even for kept runs"
    with db_engine.connect() as connection:
        remaining = connection.execute(sa.text("SELECT id FROM ingestion_runs")).scalars().all()
    assert [uuid.UUID(str(row)) for row in remaining] == [recent_run]


def test_prune_keeps_runs_that_observations_reference(db_engine: Engine) -> None:
    """The fact table records provenance, so referenced runs cannot be deleted."""
    referenced = insert_run(
        db_engine,
        started_at=NOW - timedelta(days=400),
        status="succeeded",
        finished_at=NOW - timedelta(days=400) + timedelta(minutes=1),
    )
    insert_observation(
        db_engine,
        run_id=referenced,
        observed_at=(NOW - timedelta(days=400)).replace(minute=0, second=0, microsecond=0),
    )
    tracker = RunTracker(db_engine)

    summary = tracker.prune(runs_older_than_days=365, errors_older_than_days=90, dry_run=False)

    assert summary.runs_eligible == 0
    assert summary.runs_kept_by_provenance == 1
    assert summary.runs_deleted == 0
    assert run_status(db_engine, referenced)[0] == "succeeded"


def test_prune_never_touches_running_runs(db_engine: Engine) -> None:
    stale_running = insert_run(db_engine, started_at=NOW - timedelta(days=400))

    summary = RunTracker(db_engine).prune(
        runs_older_than_days=365,
        errors_older_than_days=90,
        dry_run=False,
    )

    assert summary.runs_eligible == 0
    assert run_status(db_engine, stale_running)[0] == "running", "the reaper owns that row"


def test_prune_cli_reports_by_default_and_deletes_with_apply(
    cli_database_env: str,
    db_engine: Engine,
) -> None:
    old_run = insert_run(
        db_engine,
        started_at=NOW - timedelta(days=400),
        status="succeeded",
        finished_at=NOW - timedelta(days=400) + timedelta(minutes=1),
    )
    insert_error(db_engine, old_run, occurred_at=NOW - timedelta(days=200))

    dry = runner.invoke(app, ["db", "prune", "--json"])

    assert dry.exit_code == ExitCode.OK, dry.output
    payload = json.loads(dry.stdout)
    assert payload["dry_run"] is True
    assert payload["runs_eligible"] == 1
    assert payload["errors_eligible"] == 1

    applied = runner.invoke(app, ["db", "prune", "--apply", "--json"])

    assert applied.exit_code == ExitCode.OK, applied.output
    applied_payload = json.loads(applied.stdout)
    assert applied_payload["dry_run"] is False
    assert applied_payload["runs_deleted"] == 1
    assert applied_payload["errors_deleted"] == 1
    assert RunTracker(db_engine).count_errors(old_run) == 0


def test_prune_cli_keeps_credentials_out_of_its_output(
    monkeypatch: pytest.MonkeyPatch,
    db_engine: Engine,
) -> None:
    monkeypatch.setenv(
        "ADCP_DATABASE_URL", "postgresql+psycopg://adcp:canary-pass@localhost:59990/adcp"
    )
    monkeypatch.setenv("ADCP_DB_CONNECT_TIMEOUT_S", "1")

    result = runner.invoke(app, ["db", "prune", "--json"])

    assert result.exit_code == ExitCode.FAILURE
    assert "canary-pass" not in result.output
    assert "***" in result.output
