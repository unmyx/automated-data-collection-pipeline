"""Apply the domain rules to one fetched series.

The validator is deliberately stateless and side-effect free: it never touches the
database, never mutates its input, and never decides what to *do* about a rejection.
It answers one question - "which of these rows may be stored, and why were the
others refused?" - and hands the answer to the collection service.

Accepted rows are returned in canonical form (UTC timestamps, storage-scale
decimals) so the repository and the content hash always see the same
representation.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime
from typing import Any

from adcp.models.location import Location
from adcp.models.observation import (
    HOURLY_VARIABLES,
    ObservationSource,
    WeatherObservation,
    WeatherSeries,
)
from adcp.normalization import normalize_observation, normalize_timestamp
from adcp.pipeline.window import CollectionWindow, RowPlacement
from adcp.validation.rules import (
    FUTURE_TOLERANCE,
    Rejection,
    RejectionCode,
    check_measurement,
    rejection_code_for,
    snap_to_hour,
)


@dataclass(frozen=True, slots=True)
class ValidatedBatch:
    """The validated outcome of one location/source payload."""

    series: WeatherSeries
    accepted: tuple[WeatherObservation, ...]
    skipped: tuple[WeatherObservation, ...]
    rejections: tuple[Rejection, ...]
    #: Rows that passed every rule but were withheld because the payload as a whole
    #: crossed the rejection budget (PLAN section 9.6).
    withheld: int = 0
    #: Set when the payload itself is unusable (identity mismatch or budget breach).
    payload_failure: Rejection | None = None

    @property
    def rows_received(self) -> int:
        return len(self.accepted) + len(self.skipped) + len(self.rejections) + self.withheld

    @property
    def rows_accepted(self) -> int:
        return len(self.accepted)

    @property
    def rows_rejected(self) -> int:
        """Rows the pipeline refuses to store, including budget-withheld ones."""
        return len(self.rejections) + self.withheld

    @property
    def rows_skipped(self) -> int:
        return len(self.skipped)

    @property
    def evaluated(self) -> int:
        """Rows that were in scope for the storage window (excludes the forecast tail)."""
        return len(self.accepted) + len(self.rejections) + self.withheld

    @property
    def rejection_ratio(self) -> float:
        """Share of in-window rows that were refused, in ``[0, 1]``."""
        return 0.0 if self.evaluated == 0 else self.rows_rejected / self.evaluated

    @property
    def failed(self) -> bool:
        """Whether the payload may not be written at all."""
        return self.payload_failure is not None

    def as_dict(self) -> dict[str, Any]:
        return {
            "rows_received": self.rows_received,
            "rows_accepted": self.rows_accepted,
            "rows_rejected": self.rows_rejected,
            "rows_skipped": self.rows_skipped,
            "rejection_ratio": round(self.rejection_ratio, 4),
            "failure": None if self.payload_failure is None else self.payload_failure.as_details(),
        }


def validate_series(  # noqa: PLR0913 - the rule context is exactly this wide
    series: WeatherSeries,
    *,
    expected_location: Location,
    expected_source: ObservationSource,
    window: CollectionWindow,
    now: datetime,
    max_invalid_row_ratio: float,
) -> ValidatedBatch:
    """Validate one series against the PLAN section 9 rules.

    Args:
        series: what the adapter returned.
        expected_location: the location the request was built for.
        expected_source: the source the request was built for.
        window: storage bounds plus the tolerated payload range.
        now: reference time for the future-timestamp rule.
        max_invalid_row_ratio: the payload is refused above this rejection share.
    """
    identity_failure = _check_identity(series, expected_location, expected_source)
    if identity_failure is not None:
        return ValidatedBatch(
            series=series,
            accepted=(),
            skipped=(),
            rejections=(),
            payload_failure=identity_failure,
        )

    moment = normalize_timestamp(now)
    accepted: list[WeatherObservation] = []
    skipped: list[WeatherObservation] = []
    rejections: list[Rejection] = []
    seen: set[datetime] = set()

    for observation in series.observations:
        rejection, placement, canonical = _evaluate(
            observation,
            window=window,
            now=moment,
            seen=seen,
            slug=expected_location.slug,
            source=expected_source,
        )
        if rejection is not None:
            rejections.append(rejection)
            continue
        if placement is RowPlacement.SKIPPED:
            skipped.append(canonical)
            continue
        accepted.append(normalize_observation(canonical))

    batch = ValidatedBatch(
        series=series,
        accepted=tuple(accepted),
        skipped=tuple(skipped),
        rejections=tuple(rejections),
    )
    if batch.rejection_ratio > max_invalid_row_ratio:
        failure = Rejection(
            code=RejectionCode.REJECTION_BUDGET_EXCEEDED,
            message=(
                f"{batch.rows_rejected} of {batch.evaluated} in-window rows were rejected "
                f"({batch.rejection_ratio:.1%}), above the "
                f"{max_invalid_row_ratio:.1%} budget"
            ),
            slug=expected_location.slug,
            source=expected_source,
            fields=tuple(sorted({field for item in batch.rejections for field in item.fields})),
        )
        return ValidatedBatch(
            series=series,
            accepted=(),
            skipped=batch.skipped,
            rejections=batch.rejections,
            withheld=batch.rows_accepted,
            payload_failure=failure,
        )
    return batch


def _check_identity(
    series: WeatherSeries,
    expected_location: Location,
    expected_source: ObservationSource,
) -> Rejection | None:
    """Referential check: the payload is for the location and source we asked for."""
    if series.location.slug != expected_location.slug:
        return Rejection(
            code=RejectionCode.LOCATION_MISMATCH,
            message=(
                f"payload is for location {series.location.slug!r} but "
                f"{expected_location.slug!r} was requested"
            ),
            slug=expected_location.slug,
            source=expected_source,
            fields=("location.slug",),
        )
    if series.source is not expected_source:
        return Rejection(
            code=RejectionCode.SOURCE_MISMATCH,
            message=(
                f"payload carries source {series.source.value!r} but "
                f"{expected_source.value!r} was requested"
            ),
            slug=expected_location.slug,
            source=expected_source,
            fields=("source",),
        )
    return None


def _evaluate(  # noqa: PLR0913 - the rule context is exactly this wide
    observation: WeatherObservation,
    *,
    window: CollectionWindow,
    now: datetime,
    seen: set[datetime],
    slug: str,
    source: ObservationSource,
) -> tuple[Rejection | None, RowPlacement, WeatherObservation]:
    """Return the first rejection for a row, its placement, and its canonical form."""
    moment = normalize_timestamp(observation.observed_at)
    snapped, was_snapped = snap_to_hour(moment)
    if was_snapped:
        observation = replace(observation, observed_at=snapped)
        moment = snapped
    elif moment.minute or moment.second or moment.microsecond:
        return (
            _rejection(
                RejectionCode.NOT_HOUR_ALIGNED,
                f"timestamp {moment.isoformat()} is not aligned to the hour",
                slug,
                source,
                moment,
                ("observed_at",),
            ),
            window.placement(moment),
            observation,
        )

    # Deduplicate on the canonical timestamp: two rows that normalise to the same
    # hour would collide on the natural key inside a single batch.
    placement = window.placement(moment)
    if placement is RowPlacement.SKIPPED:
        # The provider's over-fetch (the forecast tail of today, the first partial
        # day) is not stored, so its values are not judged either. Only rows inside
        # the storage window are subject to the domain rules below.
        return None, placement, observation

    if moment in seen:
        return (
            _rejection(
                RejectionCode.DUPLICATE_TIMESTAMP,
                f"duplicate timestamp {moment.isoformat()} in one payload",
                slug,
                source,
                moment,
                ("observed_at",),
            ),
            placement,
            observation,
        )
    seen.add(moment)

    if moment > now + FUTURE_TOLERANCE:
        return (
            _rejection(
                RejectionCode.FUTURE_TIMESTAMP,
                f"timestamp {moment.isoformat()} is in the future",
                slug,
                source,
                moment,
                ("observed_at",),
            ),
            placement,
            observation,
        )

    if placement is RowPlacement.OUT_OF_RANGE:
        return (
            _rejection(
                RejectionCode.OUT_OF_WINDOW,
                (
                    f"timestamp {moment.isoformat()} is outside the requested range "
                    f"{window.accept_from.isoformat()}..{window.accept_to.isoformat()}"
                ),
                slug,
                source,
                moment,
                ("observed_at",),
            ),
            placement,
            observation,
        )

    return _check_values(observation, slug=slug, source=source), placement, observation


def _check_values(
    observation: WeatherObservation,
    *,
    slug: str,
    source: ObservationSource,
) -> Rejection | None:
    problems: list[tuple[str, str, RejectionCode]] = []
    for name in HOURLY_VARIABLES:
        value = getattr(observation, name)
        problem = check_measurement(name, value)
        if problem is not None:
            problems.append((name, problem, rejection_code_for(name, value)))
    if not problems:
        return None
    fields = tuple(name for name, _, _ in problems)
    message = "; ".join(f"{name}: {text}" for name, text, _ in problems)
    return _rejection(
        _worst_code([code for _, _, code in problems]),
        message,
        slug,
        source,
        observation.observed_at,
        fields,
    )


def _worst_code(codes: list[RejectionCode]) -> RejectionCode:
    """Prefer the most specific code when several values are out of range."""
    for candidate in (
        RejectionCode.INVALID_WEATHER_CODE,
        RejectionCode.NEGATIVE_VALUE,
        RejectionCode.OUT_OF_RANGE,
    ):
        if candidate in codes:
            return candidate
    return RejectionCode.OUT_OF_RANGE  # pragma: no cover - codes always non-empty


def _rejection(  # noqa: PLR0913, PLR0917 - a thin constructor for Rejection
    code: RejectionCode,
    message: str,
    slug: str,
    source: ObservationSource,
    observed_at: datetime,
    fields: tuple[str, ...],
) -> Rejection:
    return Rejection(
        code=code,
        message=message,
        slug=slug,
        source=source,
        observed_at=observed_at,
        fields=fields,
    )


__all__ = ["ValidatedBatch", "validate_series"]
