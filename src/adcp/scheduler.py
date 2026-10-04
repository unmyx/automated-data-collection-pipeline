"""Scheduling: a thin, safe wrapper around one-shot collection.

The scheduler owns exactly one decision - *when* to collect. Everything else
(what to fetch, how to validate it, how to write it) belongs to
:class:`adcp.pipeline.service.CollectionService`, which the scheduler reaches
through an injected ``runner`` callable. There is deliberately no collection logic
in this module (PLAN section 7.2: the scheduler only decides when; it never owns
correctness).

Three independent guards make overlapping runs impossible (PLAN section 7.4):

1. ``max_instances=1`` on the APScheduler job and a single-worker executor;
2. an in-process flag that turns an overlapping tick into a logged skip;
3. the PostgreSQL advisory lock held by the collection service itself, which also
   covers a second scheduler process, a manual ``adcp collect``, or a stale run.

The third guard is the authority: guards 1 and 2 are conveniences that keep the
log honest.
"""

from __future__ import annotations

import signal
import threading
from collections.abc import Callable, Iterator
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any, cast
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from apscheduler.events import (
    EVENT_JOB_ERROR,
    EVENT_JOB_EXECUTED,
    EVENT_JOB_MISSED,
    EVENT_SCHEDULER_STARTED,
    JobEvent,
    SchedulerEvent,
)
from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.schedulers.base import SchedulerNotRunningError
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from structlog.stdlib import BoundLogger

from adcp.config import Settings
from adcp.errors import ConfigurationError
from adcp.logging import get_logger, mask_credentials_in_text
from adcp.models.run import RunSummary

# Job identifier, also used in log events and by tests.
JOB_ID = "collect"

# PLAN section 7.3: collapse missed ticks, tolerate a 15 minute delay.
MISFIRE_GRACE_TIME_S = 900

# The scheduler waits for an in-flight collection before exiting.
SHUTDOWN_WAIT = True

# Lower bound for the development interval; nothing needs to run faster than this.
MIN_INTERVAL_SECONDS = 1
MAX_INTERVAL_SECONDS = 86_400


class SchedulerMode(StrEnum):
    """Which cadence the plan describes."""

    HOURLY = "hourly"
    INTERVAL = "interval"


@dataclass(frozen=True, slots=True)
class SchedulerPlan:
    """The resolved schedule: one trigger, fully described and validated."""

    mode: SchedulerMode
    trigger: CronTrigger | IntervalTrigger
    description: str
    timezone: str
    max_instances: int = 1
    coalesce: bool = True
    misfire_grace_time_s: int = MISFIRE_GRACE_TIME_S
    run_once: bool = False

    def as_dict(self) -> dict[str, Any]:
        """Structured fields for the startup log line."""
        return {
            "mode": self.mode.value,
            "schedule": self.description,
            "timezone": self.timezone,
            "max_instances": self.max_instances,
            "coalesce": self.coalesce,
            "misfire_grace_time_s": self.misfire_grace_time_s,
            "run_once": self.run_once,
        }


def resolve_timezone(name: str) -> ZoneInfo:
    """Validate an IANA timezone name."""
    try:
        return ZoneInfo(name)
    except (ZoneInfoNotFoundError, ValueError) as exc:
        msg = f"scheduler timezone {name!r} is not a known IANA timezone"
        raise ConfigurationError(msg) from exc


def build_schedule(
    settings: Settings,
    *,
    minute: int | None = None,
    timezone: str | None = None,
    interval_seconds: int | None = None,
    run_once: bool = False,
) -> SchedulerPlan:
    """Resolve the configured schedule into a validated :class:`SchedulerPlan`.

    CLI overrides win over settings. ``ADCP_SCHEDULER_INTERVAL_SECONDS`` (or
    ``--interval-seconds``) switches to the development cadence, which exists so a
    demonstration or a test does not have to wait an hour between cycles.
    """
    resolved_timezone = timezone if timezone is not None else settings.scheduler_timezone
    zone = resolve_timezone(resolved_timezone)

    resolved_interval = (
        interval_seconds if interval_seconds is not None else settings.scheduler_interval_seconds
    )
    if resolved_interval < 0 or resolved_interval > MAX_INTERVAL_SECONDS:
        msg = (
            f"scheduler interval must be between 0 and {MAX_INTERVAL_SECONDS} seconds, "
            f"got {resolved_interval}"
        )
        raise ConfigurationError(msg)

    resolved_minute = minute if minute is not None else settings.scheduler_minute
    if not 0 <= resolved_minute <= 59:
        msg = f"scheduler minute must be between 0 and 59, got {resolved_minute}"
        raise ConfigurationError(msg)

    if resolved_interval > 0:
        trigger: CronTrigger | IntervalTrigger = IntervalTrigger(
            seconds=resolved_interval,
            timezone=zone,
        )
        return SchedulerPlan(
            mode=SchedulerMode.INTERVAL,
            trigger=trigger,
            description=f"every {resolved_interval}s (development cadence)",
            timezone=resolved_timezone,
            run_once=run_once,
        )
    return SchedulerPlan(
        mode=SchedulerMode.HOURLY,
        trigger=CronTrigger(minute=resolved_minute, timezone=zone),
        description=f"hourly at minute {resolved_minute}",
        timezone=resolved_timezone,
        run_once=run_once,
    )


class CollectionScheduler:
    """Runs one-shot collections on a schedule.

    Args:
        plan: the resolved schedule.
        runner: callable performing exactly one collection and returning its
            summary. The CLI passes ``adcp collect``'s own helper, so scheduled
            runs and manual runs cannot drift apart.
        skip_if_running: log an overlapping tick as a skip (PLAN section 7.3).
        scheduler: injectable APScheduler instance, for tests.
    """

    def __init__(
        self,
        plan: SchedulerPlan,
        *,
        runner: Callable[[], RunSummary],
        skip_if_running: bool = True,
        scheduler: BlockingScheduler | None = None,
        logger: BoundLogger | None = None,
    ) -> None:
        self._plan = plan
        self._runner = runner
        self._skip_if_running = skip_if_running
        self._logger = logger if logger is not None else get_logger(__name__)
        self._lock = threading.Lock()
        self._running = False
        self._stopped = False
        self._runs_completed = 0
        self._previous_handlers: dict[int, Any] = {}
        self._scheduler = scheduler if scheduler is not None else self._build_scheduler()

    # -- construction ---------------------------------------------------------
    def _build_scheduler(self) -> BlockingScheduler:
        return BlockingScheduler(
            timezone=resolve_timezone(self._plan.timezone),
            # One worker: a second concurrent run is structurally impossible even
            # if a trigger ever fires twice.
            executors={"default": ThreadPoolExecutor(max_workers=1)},
            job_defaults={
                "coalesce": self._plan.coalesce,
                "max_instances": self._plan.max_instances,
                "misfire_grace_time": self._plan.misfire_grace_time_s,
            },
        )

    def configure(self) -> BlockingScheduler:
        """Register the job and the listeners. Returns the APScheduler instance."""
        self._scheduler.add_job(
            self.run_once,
            trigger=self._plan.trigger,
            id=JOB_ID,
            name=JOB_ID,
            replace_existing=True,
            coalesce=self._plan.coalesce,
            max_instances=self._plan.max_instances,
            misfire_grace_time=self._plan.misfire_grace_time_s,
        )
        self._scheduler.add_listener(
            self._on_job_event,
            EVENT_JOB_EXECUTED | EVENT_JOB_ERROR | EVENT_JOB_MISSED,
        )
        self._scheduler.add_listener(self._on_scheduler_event, EVENT_SCHEDULER_STARTED)
        return self._scheduler

    @property
    def plan(self) -> SchedulerPlan:
        return self._plan

    @property
    def runs_completed(self) -> int:
        return self._runs_completed

    @property
    def stopped(self) -> bool:
        return self._stopped

    @property
    def scheduler(self) -> BlockingScheduler:
        """The underlying APScheduler instance (tests inspect the job through it)."""
        return self._scheduler

    def next_run_time(self) -> Any:
        """When the job will fire next, or ``None`` when it will not.

        Reads the scheduler's job store, so call it from the scheduler's own
        thread or from outside event listeners - never from a job callback (see
        :meth:`_next_fire_time`).
        """
        job = self._scheduler.get_job(JOB_ID)
        return None if job is None else job.next_run_time

    def _next_fire_time(self, reference: Any = None) -> datetime | None:
        """Next fire time computed from the trigger, without touching the scheduler.

        APScheduler dispatches job events on the executor thread. Asking the
        scheduler for the job there takes the job-store lock, which the main loop
        may hold while it processes jobs - and because ``shutdown(wait=True)`` then
        waits for that executor thread, the two deadlock. Trigger arithmetic is
        pure and safe from any thread.
        """
        moment = reference
        if moment is None:
            moment = datetime.now(resolve_timezone(self._plan.timezone))
        return cast(datetime | None, self._plan.trigger.get_next_fire_time(None, moment))

    # -- lifecycle ------------------------------------------------------------
    def start(self) -> None:
        """Start the scheduler and block until shutdown.

        Startup failures raise :class:`ConfigurationError` so the CLI can exit 2;
        failures of an individual collection never reach this method (see
        :meth:`run_once`).
        """
        self.configure()
        self._install_signal_handlers()
        self._logger.info(
            "scheduler.started",
            **self._plan.as_dict(),
            skip_if_running=self._skip_if_running,
        )
        if self._plan.run_once:
            self.run_once(trigger="run_once")
        try:
            self._scheduler.start()
        finally:
            self._stopped = True
            self._restore_signal_handlers()
            self._logger.info("scheduler.stopped", runs_completed=self._runs_completed)

    def run_once(self, trigger: str = "scheduled") -> RunSummary | None:
        """Execute one collection. Never raises for a failed collection.

        Returns the run summary, or ``None`` when the tick was skipped or the
        collection failed. ``KeyboardInterrupt``/``SystemExit`` are deliberately
        not caught: a shutdown must be able to interrupt a run.
        """
        if not self._begin_run():
            self._logger.warning(
                "scheduler.collection.skipped",
                reason="stopped" if self._stopped else "already_running",
                trigger=trigger,
            )
            return None

        self._logger.info("scheduler.collection.triggered", trigger=trigger)
        try:
            summary = self._runner()
        except Exception as exc:
            # One bad cycle must not kill the scheduler; structlog's API is used
            # deliberately rather than stdlib logging.
            self._logger.error(  # noqa: TRY400
                "scheduler.collection.failed",
                trigger=trigger,
                error_type=type(exc).__name__,
                message=mask_credentials_in_text(str(exc)),
            )
            return None
        finally:
            self._end_run()

        self._runs_completed += 1
        self._logger.info(
            "scheduler.collection.completed",
            trigger=trigger,
            run_id=None if summary.run_id is None else str(summary.run_id),
            status=summary.status.value,
            **summary.counts.as_dict(),
        )
        return summary

    def shutdown(self, *, reason: str = "requested") -> None:
        """Ask APScheduler to stop, waiting for an in-flight collection to finish."""
        if self._stopped and self._scheduler.state == 0:
            return
        self._stopped = True
        self._logger.info("scheduler.shutdown.requested", reason=reason)
        self._restore_signal_handlers()
        with suppress(SchedulerNotRunningError, RuntimeError):
            self._scheduler.shutdown(wait=SHUTDOWN_WAIT)

    # -- internals ------------------------------------------------------------
    def _begin_run(self) -> bool:
        with self._lock:
            if self._stopped or self._running:
                return False
            self._running = True
            return True

    def _end_run(self) -> None:
        with self._lock:
            self._running = False

    def _install_signal_handlers(self) -> None:
        """Translate SIGINT/SIGTERM (and SIGBREAK on Windows) into a clean stop."""
        for name in ("SIGINT", "SIGTERM", "SIGBREAK"):
            signum = getattr(signal, name, None)
            if signum is None:
                continue
            try:
                self._previous_handlers[signum] = signal.signal(signum, self._handle_signal)
            except ValueError:  # pragma: no cover - not the main thread
                self._logger.debug("scheduler.signals.unavailable", signal=name)

    def _restore_signal_handlers(self) -> None:
        for signum, handler in self._previous_handlers.items():
            with suppress(ValueError, TypeError):  # pragma: no cover - not the main thread
                signal.signal(signum, handler)
        self._previous_handlers.clear()

    def _handle_signal(self, signum: int, _frame: Any) -> None:
        """Signal handler: request a shutdown and let the running job finish."""
        self.shutdown(reason=f"signal {signal.Signals(signum).name}")

    def _on_scheduler_event(self, event: SchedulerEvent) -> None:
        if event.code == EVENT_SCHEDULER_STARTED:
            self._logger.info("scheduler.next_run", next_run=str(self._next_fire_time()))

    def _on_job_event(self, event: JobEvent) -> None:
        """Log job outcomes APScheduler sees, including missed ticks."""
        if event.code == EVENT_JOB_MISSED:
            self._logger.warning(
                "scheduler.job.missed",
                job_id=event.job_id,
                scheduled_run_time=str(getattr(event, "scheduled_run_time", None)),
            )
            return
        if event.code == EVENT_JOB_ERROR:
            self._logger.error(
                "scheduler.job.error",
                job_id=event.job_id,
                error_type=type(event.exception).__name__ if event.exception else None,
                message=mask_credentials_in_text(str(event.exception)),
            )
        self._logger.info(
            "scheduler.next_run",
            next_run=str(self._next_fire_time(getattr(event, "scheduled_run_time", None))),
        )

    def iter_next_runs(self, count: int = 3) -> Iterator[Any]:
        """The next few fire times, for operator output and tests."""
        moment = self._next_fire_time()
        for _ in range(count):
            yield moment
            if moment is None:
                return
            moment = self._plan.trigger.get_next_fire_time(None, moment)


__all__ = [
    "JOB_ID",
    "MAX_INTERVAL_SECONDS",
    "MIN_INTERVAL_SECONDS",
    "MISFIRE_GRACE_TIME_S",
    "SHUTDOWN_WAIT",
    "CollectionScheduler",
    "SchedulerMode",
    "SchedulerPlan",
    "build_schedule",
    "resolve_timezone",
]
