"""One collection run, start to finish.

The service is the composition root for a run (PLAN section 4.1): it takes the
advisory lock, loads active locations and their watermarks, plans a window per
location, then - with bounded concurrency - fetches, validates, and writes each
location inside its own transaction. Failures are isolated per location, run
state is recorded as it goes, and the whole thing ends with a terminal status that
the CLI turns into an exit code.

Reliability rules this module exists to enforce:

- the advisory lock is held for the whole run, including the final accounting;
- watermarks advance in the same transaction as the rows they describe, so a crash
  before commit means the same window is simply collected again;
- rejection of one location never rolls back another;
- the failure budget turns "the API is down" into one fast failed run.
"""

from __future__ import annotations

import contextvars
import socket
import time
import uuid
from collections.abc import Callable, Iterator, Sequence
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from datetime import UTC, datetime

import structlog
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from structlog.stdlib import BoundLogger

from adcp import __version__
from adcp.config import Settings
from adcp.db.engine import connection_scope
from adcp.db.lock import COLLECTION_LOCK_KEY, advisory_lock
from adcp.db.repository import LocationRepository, WeatherRepository
from adcp.db.run_tracker import RunTracker
from adcp.db.watermark_store import WatermarkStore
from adcp.errors import AdcpError, ConfigurationError, DatabaseError, UpstreamError
from adcp.logging import get_logger, mask_credentials_in_text
from adcp.models.location import Location
from adcp.models.observation import ObservationSource
from adcp.models.run import (
    LocationResult,
    LocationStatus,
    RequestStats,
    RunCounts,
    RunStatus,
    RunSummary,
)
from adcp.pipeline.window import ScheduledWindow, plan_scheduled_window
from adcp.ports import WeatherSource
from adcp.validation.validator import ValidatedBatch, validate_series

#: How many individual row rejections are recorded per location. A payload that is
#: 90% nonsense should leave evidence without writing thousands of error rows.
REJECTION_SAMPLE_LIMIT = 25

#: Truncated error summaries still have to explain what happened.
SUMMARY_LOCATION_LIMIT = 5

#: Why a run stopped before every planned location was attempted.
STOP_FAILURE_BUDGET = "failure_budget_exceeded"
STOP_RUN_DEADLINE = "run_timeout_exceeded"

#: Error type recorded on the run row for each stop reason.
STOP_ERROR_TYPES = {
    STOP_FAILURE_BUDGET: "FailureBudgetExceeded",
    STOP_RUN_DEADLINE: "RunTimeoutExceeded",
}


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _hostname() -> str | None:
    try:
        return socket.gethostname() or None
    except OSError:  # pragma: no cover - defensive
        return None


@dataclass(frozen=True, slots=True)
class LocationPlan:
    """What one location should be asked for in this run."""

    location: Location
    source: ObservationSource
    window: ScheduledWindow


class CollectionService:
    """Executes one collection pass over the active locations."""

    def __init__(  # noqa: PLR0913 - explicit injection points, all keyword-only
        self,
        settings: Settings,
        *,
        source: WeatherSource,
        engine: Engine,
        stats: Callable[[], RequestStats] | None = None,
        now: Callable[[], datetime] = _utc_now,
        clock: Callable[[], float] = time.monotonic,
        logger: BoundLogger | None = None,
    ) -> None:
        self._settings = settings
        self._source = source
        self._engine = engine
        self._stats = stats
        self._now = now
        self._clock = clock
        self._logger = logger if logger is not None else get_logger(__name__)
        self._locations = LocationRepository(engine)
        self._weather = WeatherRepository(engine)
        self._runs = RunTracker(engine)
        self._watermarks = WatermarkStore(engine)

    def run(
        self,
        *,
        trigger: str = "cli",
        location_slugs: Sequence[str] | None = None,
        source: ObservationSource = ObservationSource.FORECAST,
        dry_run: bool = False,
    ) -> RunSummary:
        """Collect once and return the run summary.

        Args:
            trigger: how the run was started, for the audit row.
            location_slugs: restrict the run to these slugs (unknown slug = error).
            source: which upstream product to collect (only ``forecast`` is used by
                the CLI and the scheduled path; the archive and historical-forecast
                endpoints exist in the client but nothing collects them yet).
            dry_run: fetch and validate, but write nothing - not even a run row.
        """
        started_at = self._now()
        stats_before = self._snapshot_stats()
        self._logger.info("ingest.run.starting", trigger=trigger, dry_run=dry_run)

        with advisory_lock(self._engine, COLLECTION_LOCK_KEY) as lock:
            if not lock.acquired:
                self._logger.warning(
                    "ingest.run.skipped",
                    reason="lock_not_acquired",
                    trigger=trigger,
                )
                return self._finish(
                    trigger=trigger,
                    started_at=started_at,
                    results=(),
                    plans=(),
                    run_id=None,
                    dry_run=dry_run,
                    lock_acquired=False,
                    stats_before=stats_before,
                    error_summary="another run holds the collection lock",
                )

            self._reap_stale_runs()
            plans = self._plan_locations(location_slugs, source=source)
            if not plans:
                self._logger.warning("ingest.run.skipped", reason="no_active_locations")
                return self._finish(
                    trigger=trigger,
                    started_at=started_at,
                    results=(),
                    plans=(),
                    run_id=None,
                    dry_run=dry_run,
                    lock_acquired=True,
                    stats_before=stats_before,
                    error_summary="no active locations matched the request",
                )

            run_id = None if dry_run else self._start_run(trigger=trigger, plans=plans)
            # Every log line emitted from here on - including the worker threads'
            # - carries the run id (PLAN section 12.1).
            with structlog.contextvars.bound_contextvars(
                run_id=None if run_id is None else str(run_id),
            ):
                results, stop_reason = self._collect_all(plans, run_id=run_id, dry_run=dry_run)
                if stop_reason is not None and run_id is not None:
                    self._record_stop_reason(
                        run_id=run_id,
                        stop_reason=stop_reason,
                        plans=plans,
                        results=results,
                    )
                summary = self._finish(
                    trigger=trigger,
                    started_at=started_at,
                    results=results,
                    plans=plans,
                    run_id=run_id,
                    dry_run=dry_run,
                    lock_acquired=True,
                    stats_before=stats_before,
                    error_summary=None,
                    stop_reason=stop_reason,
                )

                if run_id is not None:
                    self._record_terminal_state(run_id=run_id, summary=summary)
                self._logger.info(
                    "ingest.run.completed",
                    trigger=trigger,
                    dry_run=dry_run,
                    stop_reason=stop_reason,
                    **summary.counts.as_dict(),
                )
            return summary

    def _reap_stale_runs(self) -> None:
        """Fail runs that were left ``running`` by a killed process.

        Runs inside the advisory lock at the start of every collection, so a
        crashed run cannot make the run table look healthy forever (PLAN section
        11.3). Reaping failures are logged, never fatal to the current run.
        """
        max_age_s = 2 * self._settings.run_timeout_s
        try:
            reaped = self._runs.reap_stale_runs(max_age_s=max_age_s)
        except SQLAlchemyError as exc:
            self._logger.warning(
                "ingest.runs.reap_failed",
                error_type=type(exc).__name__,
                message=mask_credentials_in_text(str(exc)),
            )
            return
        if reaped:
            self._logger.warning(
                "ingest.runs.reaped",
                count=len(reaped),
                run_ids=[str(run_id) for run_id in reaped],
                max_age_s=max_age_s,
            )

    def _plan_locations(
        self,
        slugs: Sequence[str] | None,
        *,
        source: ObservationSource,
    ) -> tuple[LocationPlan, ...]:
        """Load active locations, read their watermarks, and plan each window."""
        loaded = [Location.from_record(record) for record in self._locations.list_active()]
        if slugs is not None:
            requested = list(dict.fromkeys(slugs))
            known = {location.slug for location in loaded}
            unknown = [slug for slug in requested if slug not in known]
            if unknown:
                msg = f"unknown or inactive location(s): {', '.join(unknown)}"
                raise ConfigurationError(msg)
            loaded = [location for location in loaded if location.slug in set(requested)]

        moment = self._now()
        plans: list[LocationPlan] = []
        for location in loaded:
            if location.id is None:
                msg = (
                    f"location {location.slug!r} has no database id; collection needs "
                    "locations loaded from the database"
                )
                raise ConfigurationError(msg)
            watermark = self._watermarks.get(location_id=location.id, source=source.value)
            window = plan_scheduled_window(
                now=moment,
                watermark=None if watermark is None else watermark.last_observed_at,
                lookback_hours=self._settings.ingest_lookback_hours,
                overlap_hours=self._settings.ingest_overlap_hours,
            )
            if window.truncated:
                self._logger.warning(
                    "ingest.run.window_truncated",
                    location=location.slug,
                    storage_hours=window.bounds.hours,
                    lookback_hours=self._settings.ingest_lookback_hours,
                )
            plans.append(LocationPlan(location=location, source=source, window=window))
        return tuple(plans)

    def _start_run(self, *, trigger: str, plans: Sequence[LocationPlan]) -> uuid.UUID:
        return self._runs.start_run(
            run_type="scheduled" if trigger == "scheduler" else "manual",
            trigger=trigger,
            app_version=__version__,
            window_from=min(plan.window.bounds.storage_start for plan in plans),
            window_to=max(plan.window.bounds.storage_end for plan in plans),
            hostname=_hostname(),
        ).id

    def _collect_all(
        self,
        plans: Sequence[LocationPlan],
        *,
        run_id: uuid.UUID | None,
        dry_run: bool,
    ) -> tuple[tuple[LocationResult, ...], str | None]:
        """Process locations with bounded concurrency, a failure budget, and a deadline.

        Returns the results plus why scheduling stopped early (``None`` when every
        location was attempted). The deadline is the run wall-clock budget from
        PLAN section 8.5: in-flight locations still finish and commit, but no new
        one starts, so a slow provider cannot turn one run into a multi-hour one.
        """
        results: list[LocationResult] = []
        failures = 0
        budget = self._settings.ingest_failure_budget_ratio
        stop_reason: str | None = None
        deadline = self._clock() + self._settings.run_timeout_s
        pending: dict[Future[LocationResult], LocationPlan] = {}
        queue: Iterator[LocationPlan] = iter(plans)
        workers = max(1, self._settings.open_meteo_max_concurrency)

        with ThreadPoolExecutor(
            max_workers=workers,
            thread_name_prefix="adcp-collect",
        ) as pool:
            while True:
                while stop_reason is None and len(pending) < workers:
                    plan = next(queue, None)
                    if plan is None:
                        break
                    # Run each location in a copy of this thread's context so the
                    # bound run id (and the service/env context from configure_logging)
                    # reaches the worker's log lines too.
                    task_context = contextvars.copy_context()
                    pending[
                        pool.submit(
                            task_context.run,
                            self._collect_location,
                            plan,
                            run_id,
                            dry_run,
                        )
                    ] = plan
                if not pending:
                    break
                done, _ = wait(set(pending), return_when=FIRST_COMPLETED)
                for future in done:
                    pending.pop(future, None)
                    result = future.result()
                    results.append(result)
                    if result.failed:
                        failures += 1
                ratio = failures / len(plans) if plans else 0.0
                if stop_reason is None and ratio > budget:
                    stop_reason = STOP_FAILURE_BUDGET
                    self._logger.error(
                        "ingest.run.failure_budget_exceeded",
                        failed=failures,
                        requested=len(plans),
                        ratio=budget,
                    )
                    for skipped_plan in queue:
                        self._logger.warning(
                            "ingest.location.skipped",
                            location=skipped_plan.location.slug,
                            reason="failure_budget_exceeded",
                        )
                    queue = iter(())
                elif stop_reason is None and self._clock() >= deadline:
                    stop_reason = STOP_RUN_DEADLINE
                    self._logger.error(
                        "ingest.run.deadline_exceeded",
                        elapsed_s=round(
                            self._clock() - (deadline - self._settings.run_timeout_s), 1
                        ),
                        run_timeout_s=self._settings.run_timeout_s,
                    )
                    for skipped_plan in queue:
                        self._logger.warning(
                            "ingest.location.skipped",
                            location=skipped_plan.location.slug,
                            reason="run_deadline_exceeded",
                        )
                    queue = iter(())
        return tuple(results), stop_reason

    def _collect_location(
        self,
        plan: LocationPlan,
        run_id: uuid.UUID | None,
        dry_run: bool,
    ) -> LocationResult:
        """Fetch, validate, and persist one location. Never raises."""
        with structlog.contextvars.bound_contextvars(
            location=plan.location.slug,
            source=plan.source.value,
        ):
            return self._collect_location_inner(plan, run_id, dry_run)

    def _collect_location_inner(
        self,
        plan: LocationPlan,
        run_id: uuid.UUID | None,
        dry_run: bool,
    ) -> LocationResult:
        started = self._clock()
        location = plan.location
        self._logger.info(
            "ingest.location.started",
            location=location.slug,
            source=plan.source.value,
            **plan.window.bounds.as_dict(),
        )
        try:
            series = self._source.fetch_hourly(
                location=location,
                source=plan.source,
                window=plan.window.request,
            )
        except UpstreamError as exc:
            return self._fail(
                plan,
                started=started,
                phase="fetch",
                error_type=type(exc).__name__,
                message=str(exc),
                run_id=run_id,
                http_status=exc.status_code,
                request_url=exc.endpoint,
                payload_sample=exc.as_details(),
            )
        except AdcpError as exc:
            return self._fail(
                plan,
                started=started,
                phase="fetch",
                error_type=type(exc).__name__,
                message=str(exc),
                run_id=run_id,
            )
        except Exception as exc:
            return self._fail(
                plan,
                started=started,
                phase="fetch",
                error_type=type(exc).__name__,
                message=f"unexpected error while fetching: {exc}",
                run_id=run_id,
            )

        batch = validate_series(
            series,
            expected_location=location,
            expected_source=plan.source,
            window=plan.window.bounds,
            now=self._now(),
            max_invalid_row_ratio=self._settings.ingest_max_invalid_row_ratio,
        )
        self._record_rejections(plan, batch, run_id=run_id)

        if batch.failed:
            failure = batch.payload_failure
            return self._fail(
                plan,
                started=started,
                phase="validate",
                error_type=failure.code.value if failure else "ValidationFailure",
                message=failure.message if failure else "payload rejected",
                run_id=run_id,
                payload_sample=None if failure is None else failure.as_details(),
                batch=batch,
            )

        if dry_run:
            self._logger.info(
                "ingest.location.validated",
                dry_run=True,
                location=location.slug,
                **batch.as_dict(),
            )
            return LocationResult(
                slug=location.slug,
                source=plan.source,
                status=LocationStatus.SUCCEEDED,
                rows_received=batch.rows_received,
                rows_accepted=batch.rows_accepted,
                rows_rejected=batch.rows_rejected,
                rows_skipped=batch.rows_skipped,
                duration_ms=self._duration_ms(started),
            )

        if run_id is None:  # pragma: no cover - a real run always has one
            msg = "refusing to persist without a run id"
            raise ConfigurationError(msg)
        return self._persist(plan, batch, run_id=run_id, started=started)

    def _persist(
        self,
        plan: LocationPlan,
        batch: ValidatedBatch,
        *,
        run_id: uuid.UUID,
        started: float,
    ) -> LocationResult:
        """Write one location's rows and watermark in a single transaction."""
        location = plan.location
        if location.id is None:  # pragma: no cover - guarded while planning
            msg = f"location {location.slug!r} has no database id"
            raise ConfigurationError(msg)
        try:
            with connection_scope(self._engine) as connection:
                counts = self._weather.upsert_observations(
                    connection,
                    location_id=location.id,
                    run_id=run_id,
                    series=batch.series,
                    observations=batch.accepted,
                )
                watermark = max(
                    (observation.observed_at for observation in batch.accepted),
                    default=None,
                )
                if watermark is not None:
                    self._watermarks.advance(
                        connection,
                        location_id=location.id,
                        source=plan.source,
                        last_observed_at=watermark,
                        run_id=run_id,
                    )
        except Exception as exc:
            return self._fail(
                plan,
                started=started,
                phase="write",
                error_type=type(exc).__name__,
                message=f"failed to persist observations: {exc}",
                run_id=run_id,
                batch=batch,
            )

        result = LocationResult(
            slug=location.slug,
            source=plan.source,
            status=LocationStatus.SUCCEEDED,
            rows_received=batch.rows_received,
            rows_accepted=batch.rows_accepted,
            rows_inserted=counts.inserted,
            rows_updated=counts.updated,
            rows_unchanged=counts.unchanged,
            rows_rejected=batch.rows_rejected,
            rows_skipped=batch.rows_skipped,
            duration_ms=self._duration_ms(started),
            watermark=watermark,
        )
        self._logger.info("ingest.location.completed", **result.as_dict())
        return result

    def _fail(  # noqa: PLR0913 - collects the error context for one location
        self,
        plan: LocationPlan,
        *,
        started: float,
        phase: str,
        error_type: str,
        message: str,
        run_id: uuid.UUID | None,
        http_status: int | None = None,
        request_url: str | None = None,
        payload_sample: dict[str, object] | None = None,
        batch: ValidatedBatch | None = None,
    ) -> LocationResult:
        """Record a failed location and return its result."""
        self._record_error(
            run_id=run_id,
            plan=plan,
            phase=phase,
            error_type=error_type,
            message=message,
            http_status=http_status,
            request_url=request_url,
            payload_sample=payload_sample,
        )
        result = LocationResult(
            slug=plan.location.slug,
            source=plan.source,
            status=LocationStatus.FAILED,
            rows_received=0 if batch is None else batch.rows_received,
            rows_accepted=0 if batch is None else batch.rows_accepted,
            rows_rejected=0 if batch is None else batch.rows_rejected,
            rows_skipped=0 if batch is None else batch.rows_skipped,
            duration_ms=self._duration_ms(started),
            error_type=error_type,
            error_message=message,
        )
        self._logger.error("ingest.location.failed", **result.as_dict())
        return result

    def _record_rejections(
        self,
        plan: LocationPlan,
        batch: ValidatedBatch,
        *,
        run_id: uuid.UUID | None,
    ) -> None:
        """Persist individual rejections, bounded per location."""
        for rejection in batch.rejections[:REJECTION_SAMPLE_LIMIT]:
            self._record_error(
                run_id=run_id,
                plan=plan,
                phase="validate",
                error_type=rejection.code.value,
                message=rejection.message,
                payload_sample=rejection.as_details(),
            )
        omitted = len(batch.rejections) - REJECTION_SAMPLE_LIMIT
        if omitted > 0:
            self._record_error(
                run_id=run_id,
                plan=plan,
                phase="validate",
                error_type="RejectionsOmitted",
                message=f"{omitted} further rejected rows were not recorded individually",
                payload_sample={"omitted": omitted, "location": plan.location.slug},
            )

    def _record_error(  # noqa: PLR0913 - mirrors the ingestion_run_errors columns
        self,
        *,
        run_id: uuid.UUID | None,
        plan: LocationPlan,
        phase: str,
        error_type: str,
        message: str,
        http_status: int | None = None,
        request_url: str | None = None,
        payload_sample: dict[str, object] | None = None,
    ) -> None:
        """Write one ``ingestion_run_errors`` row in its own transaction.

        Error recording must never take the run down with it, so failures are
        logged and swallowed. If the location row has vanished (the referential
        failure being recorded), the row is retried without a location id.
        """
        if run_id is None:
            return
        for location_id in (plan.location.id, None):
            try:
                with connection_scope(self._engine) as connection:
                    self._runs.record_error(
                        connection,
                        run_id,
                        phase=phase,
                        error_type=error_type,
                        message=message,
                        location_id=location_id,
                        http_status=http_status,
                        request_url=request_url,
                        payload_sample=payload_sample,
                    )
            except SQLAlchemyError as exc:
                self._logger.error(  # noqa: TRY400 - structlog API, not stdlib logging
                    "ingest.error_record.failed",
                    location=plan.location.slug,
                    error_type=type(exc).__name__,
                    message=str(exc),
                )
            else:
                return

    def _record_terminal_state(self, *, run_id: uuid.UUID, summary: RunSummary) -> None:
        try:
            with connection_scope(self._engine) as connection:
                self._runs.finish_run(
                    connection,
                    run_id,
                    status=summary.status,
                    counts=summary.counts.to_db_columns(),
                    error_summary=summary.error_summary,
                    duration_ms=summary.duration_ms,
                )
        except DatabaseError as exc:
            self._logger.error(  # noqa: TRY400 - structlog API, not stdlib logging
                "ingest.run.finalise_failed",
                run_id=str(run_id),
                error_type=type(exc).__name__,
                message=str(exc),
            )

    def _record_stop_reason(
        self,
        *,
        run_id: uuid.UUID,
        stop_reason: str,
        plans: Sequence[LocationPlan],
        results: Sequence[LocationResult],
    ) -> None:
        """Record why a run stopped early, on the run's own error trail."""
        unattempted = [plan.location.slug for plan in plans]
        attempted = {result.slug for result in results}
        remaining = [slug for slug in unattempted if slug not in attempted]
        error_type = STOP_ERROR_TYPES.get(stop_reason, "RunStoppedEarly")
        message = (
            f"the run stopped early ({stop_reason}) after {len(results)} of "
            f"{len(plans)} locations; not attempted: {', '.join(remaining) or '(none)'}"
        )
        try:
            with connection_scope(self._engine) as connection:
                self._runs.record_error(
                    connection,
                    run_id,
                    phase="run",
                    error_type=error_type,
                    message=message,
                    error_code=stop_reason,
                    payload_sample={
                        "stop_reason": stop_reason,
                        "attempted": sorted(attempted),
                        "not_attempted": remaining,
                    },
                )
        except SQLAlchemyError as exc:  # pragma: no cover - defensive
            self._logger.warning(
                "ingest.error_record.failed",
                error_type=type(exc).__name__,
                message=str(exc),
                stop_reason=stop_reason,
            )

    def _finish(  # noqa: PLR0913 - assembles the run summary from its inputs
        self,
        *,
        trigger: str,
        started_at: datetime,
        results: tuple[LocationResult, ...],
        plans: tuple[LocationPlan, ...],
        run_id: uuid.UUID | None,
        dry_run: bool,
        lock_acquired: bool,
        stats_before: RequestStats | None,
        error_summary: str | None,
        stop_reason: str | None = None,
    ) -> RunSummary:
        stats_delta = self._stats_delta(stats_before)
        counts = RunCounts.from_results(
            results,
            locations_total=len(plans),
            requests_made=stats_delta.requests_made,
            requests_retried=stats_delta.requests_retried,
        )
        status = RunStatus.SKIPPED
        if plans:
            status, budget_failure, summary = compute_run_status(
                results,
                locations_requested=len(plans),
                failure_budget_ratio=self._settings.ingest_failure_budget_ratio,
            )
            error_summary = error_summary or summary
            if budget_failure:
                self._logger.error(
                    "ingest.run.failed",
                    reason="failure_budget_exceeded",
                    locations_total=len(plans),
                )
        if stop_reason is not None:
            # A run that could not attempt every planned location is not a
            # success, whatever the per-location outcomes were.
            status = RunStatus.FAILED
            error_summary = error_summary or (
                f"the run stopped early ({stop_reason}) after {len(results)} of "
                f"{len(plans)} locations"
            )
        return RunSummary(
            status=status,
            trigger=trigger,
            started_at=started_at,
            finished_at=self._now(),
            counts=counts,
            run_id=run_id,
            lock_acquired=lock_acquired,
            dry_run=dry_run,
            window_truncated=any(plan.window.truncated for plan in plans),
            error_summary=error_summary,
            results=results,
        )

    def _duration_ms(self, started: float) -> int:
        return int((self._clock() - started) * 1_000)

    def _snapshot_stats(self) -> RequestStats | None:
        return None if self._stats is None else self._stats()

    def _stats_delta(self, before: RequestStats | None) -> RequestStats:
        if before is None or self._stats is None:
            return RequestStats()
        return before.delta(self._stats())


def compute_run_status(
    results: Sequence[LocationResult],
    *,
    locations_requested: int,
    failure_budget_ratio: float,
) -> tuple[RunStatus, bool, str | None]:
    """Derive the terminal status, whether the budget tripped, and a summary line.

    PLAN section 11.3: partial means "some data written, something failed"; a
    failure share above the budget turns the whole run into a failure so an outage
    is one fast failed run instead of a slow trickle of partials.
    """
    if locations_requested <= 0:
        return RunStatus.SKIPPED, False, "no active locations matched the request"
    if not results:
        return RunStatus.FAILED, False, "no locations were attempted"

    failed = [result for result in results if result.failed]
    succeeded = [result for result in results if result.succeeded]
    rejected = sum(result.rows_rejected for result in results)
    unattempted = locations_requested - len(results)

    if len(failed) / locations_requested > failure_budget_ratio:
        names = ", ".join(result.slug for result in failed[:SUMMARY_LOCATION_LIMIT])
        summary = (
            f"{len(failed)} of {locations_requested} locations failed "
            f"(budget {failure_budget_ratio:.0%}); first failures: {names}"
        )
        return RunStatus.FAILED, True, summary

    if failed and not succeeded:
        names = ", ".join(result.slug for result in failed[:SUMMARY_LOCATION_LIMIT])
        return RunStatus.FAILED, False, f"every location failed: {names}"

    if failed or rejected:
        parts: list[str] = []
        if failed:
            parts.append(f"{len(failed)} location(s) failed")
        if rejected:
            parts.append(f"{rejected} row(s) rejected")
        if unattempted:
            parts.append(f"{unattempted} location(s) not attempted")
        return RunStatus.PARTIAL, False, "; ".join(parts)

    return RunStatus.SUCCEEDED, False, None


__all__ = [
    "REJECTION_SAMPLE_LIMIT",
    "CollectionService",
    "LocationPlan",
    "compute_run_status",
]
