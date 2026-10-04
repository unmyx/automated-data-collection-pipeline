"""The scheduler: plan construction, job invocation, isolation, and shutdown."""

from __future__ import annotations

import io
import signal
import threading
from typing import Any, cast

import pytest
from apscheduler.events import (
    EVENT_JOB_EXECUTED,
    EVENT_JOB_MISSED,
    EVENT_SCHEDULER_STARTED,
    JobEvent,
    SchedulerEvent,
)
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from adcp.config import Settings
from adcp.errors import ConfigurationError, UpstreamServerError
from adcp.logging import configure_logging, get_logger
from adcp.models.run import RunStatus, RunSummary
from adcp.scheduler import (
    JOB_ID,
    MISFIRE_GRACE_TIME_S,
    CollectionScheduler,
    SchedulerMode,
    build_schedule,
    resolve_timezone,
)
from tests.support import events_named, run_summary

pytestmark = pytest.mark.unit


class StubRunner:
    """Records calls and returns (or raises) whatever the test scripted."""

    def __init__(self, *outcomes: RunSummary | Exception, gate: threading.Event | None = None):
        self._outcomes = list(outcomes)
        self._gate = gate
        self.calls = 0

    def __call__(self) -> RunSummary:
        self.calls += 1
        if self._gate is not None:
            self._gate.wait(timeout=5)
        outcome = self._outcomes.pop(0) if self._outcomes else run_summary()
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


class FakeApscheduler:
    """A duck-typed stand-in, so shutdown semantics can be asserted exactly."""

    def __init__(self) -> None:
        self.state = 0
        self.jobs: dict[str, Any] = {}
        self.listeners: list[tuple[Any, int]] = []
        self.shutdown_calls: list[bool] = []
        self.started = False

    def add_job(self, func: Any, **kwargs: Any) -> None:
        self.jobs[kwargs["id"]] = type("Job", (), {**kwargs, "func": func})()

    def add_listener(self, callback: Any, mask: int) -> None:
        self.listeners.append((callback, mask))

    def get_job(self, job_id: str) -> Any:
        return self.jobs.get(job_id)

    def start(self, *_args: Any, **_kwargs: Any) -> None:
        self.started = True
        self.state = 1

    def shutdown(self, wait: bool = True) -> None:
        self.shutdown_calls.append(wait)
        self.state = 0


class StrictApscheduler(FakeApscheduler):
    """A scheduler that refuses job lookups, like the deadlock case would."""

    def get_job(self, _job_id: str) -> Any:
        raise AssertionError(
            "event listeners must not touch the scheduler's job store: "
            "the main loop may hold that lock, which deadlocks shutdown(wait=True)",
        )


@pytest.fixture
def settings() -> Settings:
    return Settings(_env_file=None, scheduler_enabled=True, scheduler_minute=7)


@pytest.fixture
def logs() -> io.StringIO:
    buffer = io.StringIO()
    configure_logging(
        level="DEBUG", log_format="json", service="adcp", environment="test", stream=buffer
    )
    return buffer


def scheduler_for(
    settings: Settings,
    runner: Any,
    apscheduler: FakeApscheduler | None = None,
    **plan_overrides: Any,
) -> tuple[CollectionScheduler, FakeApscheduler]:
    plan = build_schedule(settings, **plan_overrides)
    fake = apscheduler if apscheduler is not None else FakeApscheduler()
    scheduler = CollectionScheduler(
        plan,
        runner=runner,
        scheduler=cast(BlockingScheduler, fake),
        skip_if_running=settings.scheduler_skip_if_running,
        logger=get_logger("tests.scheduler"),
    )
    return scheduler, fake


# -- plan construction ---------------------------------------------------------
def test_the_default_plan_is_the_hourly_cron_model(settings: Settings) -> None:
    plan = build_schedule(settings)

    assert plan.mode is SchedulerMode.HOURLY
    assert isinstance(plan.trigger, CronTrigger)
    assert plan.description == "hourly at minute 7"
    fields = {
        field.name: sorted(str(item) for item in field.expressions) for field in plan.trigger.fields
    }
    assert fields["minute"] == ["7"]
    assert fields["hour"] == ["*"]
    assert plan.max_instances == 1
    assert plan.coalesce is True
    assert plan.misfire_grace_time_s == MISFIRE_GRACE_TIME_S
    assert plan.run_once is False


def test_cli_overrides_win_over_settings(settings: Settings) -> None:
    plan = build_schedule(
        settings,
        minute=30,
        timezone="Europe/Belgrade",
        run_once=True,
    )

    assert plan.timezone == "Europe/Belgrade"
    assert str(plan.trigger.timezone) == "Europe/Belgrade"
    assert plan.run_once is True
    fields = {
        field.name: sorted(str(item) for item in field.expressions) for field in plan.trigger.fields
    }
    assert fields["minute"] == ["30"]


def test_interval_mode_is_configuration_driven() -> None:
    from_settings = Settings(_env_file=None, scheduler_interval_seconds=15)
    from_cli = Settings(_env_file=None)

    assert build_schedule(from_settings).mode is SchedulerMode.INTERVAL
    plan = build_schedule(from_cli, interval_seconds=5)

    assert plan.mode is SchedulerMode.INTERVAL
    assert isinstance(plan.trigger, IntervalTrigger)
    assert plan.trigger.interval.total_seconds() == 5
    assert "every 5s" in plan.description


def test_interval_mode_wins_over_the_hourly_minute(settings: Settings) -> None:
    plan = build_schedule(settings, interval_seconds=30)

    assert plan.mode is SchedulerMode.INTERVAL


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"minute": 60}, "minute"),
        ({"minute": -1}, "minute"),
        ({"timezone": "Mars/Olympus_Mons"}, "timezone"),
        ({"interval_seconds": -1}, "interval"),
        ({"interval_seconds": 100_000}, "interval"),
    ],
)
def test_invalid_schedule_configuration_is_rejected(
    settings: Settings,
    overrides: dict[str, Any],
    message: str,
) -> None:
    with pytest.raises(ConfigurationError, match=message):
        build_schedule(settings, **overrides)


def test_timezone_resolution_reports_unknown_names() -> None:
    with pytest.raises(ConfigurationError, match="IANA"):
        resolve_timezone("Nowhere/At_All")


def test_plan_serialises_for_the_startup_log(settings: Settings) -> None:
    described = build_schedule(settings).as_dict()

    assert described["mode"] == "hourly"
    assert described["schedule"] == "hourly at minute 7"
    assert described["max_instances"] == 1
    assert described["run_once"] is False


# -- job registration and invocation -------------------------------------------
def test_the_job_is_registered_with_the_plan_settings(
    settings: Settings,
    logs: io.StringIO,
) -> None:
    scheduler, fake = scheduler_for(
        settings,
        StubRunner(),
    )

    scheduler.configure()

    job = fake.jobs[JOB_ID]
    assert job.max_instances == 1
    assert job.coalesce is True
    assert job.misfire_grace_time == MISFIRE_GRACE_TIME_S
    assert isinstance(job.trigger, CronTrigger)
    assert [mask for _, mask in fake.listeners], "listeners are registered"


def test_run_once_invokes_the_runner_and_logs_the_outcome(
    settings: Settings,
    logs: io.StringIO,
) -> None:
    runner = StubRunner(run_summary(rows_inserted=5, rows_received=5))
    scheduler, _ = scheduler_for(
        settings,
        runner,
    )

    summary = scheduler.run_once(trigger="scheduled")

    assert runner.calls == 1
    assert summary is not None
    assert scheduler.runs_completed == 1
    assert len(events_named(logs, "scheduler.collection.triggered")) == 1
    completed = events_named(logs, "scheduler.collection.completed")
    assert completed[0]["status"] == "succeeded"
    assert completed[0]["rows_inserted"] == 5
    assert completed[0]["run_id"] is not None


def test_a_failing_collection_is_logged_and_does_not_stop_the_scheduler(
    settings: Settings,
    logs: io.StringIO,
) -> None:
    runner = StubRunner(
        UpstreamServerError("Open-Meteo returned a server error (HTTP 503)"),
        run_summary(rows_inserted=2, rows_received=2),
    )
    scheduler, _ = scheduler_for(
        settings,
        runner,
    )

    failed = scheduler.run_once()
    recovered = scheduler.run_once()

    assert failed is None
    assert recovered is not None, "the next cycle still runs"
    assert runner.calls == 2
    assert scheduler.runs_completed == 1, "the failed cycle is not counted as completed"
    failures = events_named(logs, "scheduler.collection.failed")
    assert len(failures) == 1
    assert failures[0]["error_type"] == "UpstreamServerError"
    assert len(events_named(logs, "scheduler.collection.completed")) == 1


def test_a_failed_run_summary_is_reported_but_not_raised(
    settings: Settings,
    logs: io.StringIO,
) -> None:
    scheduler, _ = scheduler_for(
        settings,
        StubRunner(run_summary(status=RunStatus.FAILED)),
    )

    summary = scheduler.run_once()

    assert summary is not None
    assert summary.status is RunStatus.FAILED
    assert events_named(logs, "scheduler.collection.completed")[0]["status"] == "failed"
    assert events_named(logs, "scheduler.collection.failed") == []


def test_overlapping_ticks_are_skipped(
    settings: Settings,
    logs: io.StringIO,
) -> None:
    gate = threading.Event()
    runner = StubRunner(run_summary(), gate=gate)
    scheduler, _ = scheduler_for(
        settings,
        runner,
    )
    thread = threading.Thread(target=scheduler.run_once, kwargs={"trigger": "scheduled"})

    thread.start()
    try:
        overlaps = [scheduler.run_once(trigger="scheduled") for _ in range(3)]
    finally:
        gate.set()
        thread.join(timeout=5)

    assert overlaps == [None, None, None]
    assert runner.calls == 1, "only one collection ran"
    skipped = events_named(logs, "scheduler.collection.skipped")
    assert len(skipped) == 3
    assert {event["reason"] for event in skipped} == {"already_running"}


def test_ticks_after_shutdown_are_skipped(
    settings: Settings,
    logs: io.StringIO,
) -> None:
    runner = StubRunner(run_summary())
    scheduler, fake = scheduler_for(
        settings,
        runner,
    )

    scheduler.shutdown(reason="test")
    result = scheduler.run_once()

    assert result is None
    assert runner.calls == 0
    assert fake.shutdown_calls == [True]
    assert events_named(logs, "scheduler.collection.skipped")[0]["reason"] == "stopped"


def test_lock_contention_is_reported_as_a_skipped_run(
    settings: Settings,
    logs: io.StringIO,
) -> None:
    """The service returns a skipped summary when another run holds the lock."""
    scheduler, _ = scheduler_for(
        settings,
        StubRunner(run_summary(status=RunStatus.SKIPPED)),
    )

    summary = scheduler.run_once()

    assert summary is not None
    assert summary.status is RunStatus.SKIPPED
    assert events_named(logs, "scheduler.collection.completed")[0]["status"] == "skipped"


# -- shutdown ------------------------------------------------------------------
def test_shutdown_waits_for_the_running_collection(
    settings: Settings,
    logs: io.StringIO,
) -> None:
    scheduler, fake = scheduler_for(
        settings,
        StubRunner(),
    )

    scheduler.shutdown(reason="signal SIGTERM")
    scheduler.shutdown(reason="again")

    assert fake.shutdown_calls == [True], "wait=True exactly once"
    assert scheduler.stopped is True
    assert len(events_named(logs, "scheduler.shutdown.requested")) == 1
    assert events_named(logs, "scheduler.shutdown.requested")[0]["reason"] == "signal SIGTERM"


def test_signal_handler_requests_a_clean_shutdown(
    settings: Settings,
    logs: io.StringIO,
) -> None:
    scheduler, fake = scheduler_for(
        settings,
        StubRunner(),
    )

    scheduler._handle_signal(signal.SIGINT, None)

    assert scheduler.stopped is True
    assert fake.shutdown_calls == [True]
    assert "SIGINT" in events_named(logs, "scheduler.shutdown.requested")[0]["reason"]


def test_signal_handlers_are_installed_and_restored(
    settings: Settings,
    logs: io.StringIO,
) -> None:
    scheduler, _ = scheduler_for(
        settings,
        StubRunner(),
    )
    original = signal.getsignal(signal.SIGINT)

    scheduler._install_signal_handlers()
    installed = signal.getsignal(signal.SIGINT)
    scheduler.shutdown(reason="test")

    assert installed is not original
    assert signal.getsignal(signal.SIGINT) is original


def test_start_installs_handlers_runs_once_and_stops(
    settings: Settings,
    logs: io.StringIO,
) -> None:
    runner = StubRunner(run_summary())
    scheduler, fake = scheduler_for(settings, runner, run_once=True)
    original = signal.getsignal(signal.SIGINT)

    scheduler.start()  # the fake scheduler returns immediately

    assert runner.calls == 1, "--run-once collects before scheduling"
    assert fake.started is True
    assert scheduler.stopped is True
    assert signal.getsignal(signal.SIGINT) is original
    assert events_named(logs, "scheduler.started")[0]["mode"] == "hourly"
    assert len(events_named(logs, "scheduler.stopped")) == 1


def test_missed_job_events_are_logged(
    settings: Settings,
    logs: io.StringIO,
) -> None:
    scheduler, _ = scheduler_for(
        settings,
        StubRunner(),
    )
    event = JobEvent(EVENT_JOB_MISSED, JOB_ID, "default")

    scheduler._on_job_event(event)

    missed = events_named(logs, "scheduler.job.missed")
    assert len(missed) == 1
    assert missed[0]["job_id"] == JOB_ID


def test_event_listeners_never_look_up_jobs_on_the_scheduler(
    settings: Settings,
    logs: io.StringIO,
) -> None:
    """Regression: job callbacks must use trigger arithmetic, not ``get_job()``.

    APScheduler dispatches job events on the executor thread. Looking the job up
    there takes the job-store lock that the main loop may hold, and because
    ``shutdown(wait=True)`` waits for the executor thread, the two deadlock -
    which is exactly what the integration cycle tests caught.
    """
    scheduler, _ = scheduler_for(settings, StubRunner(), apscheduler=StrictApscheduler())

    scheduler._on_job_event(JobEvent(EVENT_JOB_EXECUTED, JOB_ID, "default"))
    scheduler._on_job_event(JobEvent(EVENT_JOB_MISSED, JOB_ID, "default"))
    scheduler._on_scheduler_event(SchedulerEvent(EVENT_SCHEDULER_STARTED))
    upcoming = list(scheduler.iter_next_runs(count=3))

    assert len(upcoming) == 3
    assert all(moment is not None for moment in upcoming)
    # EXECUTED and SCHEDULER_STARTED log the next fire time; MISSED logs itself.
    assert len(events_named(logs, "scheduler.next_run")) == 2
    assert len(events_named(logs, "scheduler.job.missed")) == 1


def test_credentials_are_masked_in_scheduler_logs(
    settings: Settings,
    logs: io.StringIO,
) -> None:
    runner = StubRunner(
        UpstreamServerError(
            "failed to call https://api.open-meteo.com/v1/forecast?apikey=canary-key-789",
        ),
    )
    scheduler, _ = scheduler_for(
        settings,
        runner,
    )

    scheduler.run_once()

    logged = logs.getvalue()
    assert "canary-key-789" not in logged
    assert "apikey=***" in logged
