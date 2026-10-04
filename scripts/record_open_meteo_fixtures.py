"""Record the Open-Meteo contract fixtures used by the test suite.

Run this deliberately, review the diff, and commit - never from a test run:

    python scripts/record_open_meteo_fixtures.py

Why a recorder instead of hand-written JSON:

- the "valid" fixtures are real provider responses, so the parser is tested
  against the actual envelope, unit spellings, and timestamp format;
- the anomaly fixtures are *derived* from the recorded payload, so each one
  breaks exactly one documented thing and cannot drift out of shape;
- re-recording is a reviewable diff, which is how the project notices provider
  contract changes (PLAN section 14.4).

Fixtures are written to ``tests/fixtures/open_meteo/``.
"""

from __future__ import annotations

import argparse
import copy
import json
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx

from adcp import __version__
from adcp.models.observation import HOURLY_VARIABLES

FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"

# Belgrade, Serbia - the location used throughout the fixtures.
LATITUDE = 44.8125
LONGITUDE = 20.4375

# Small enough to review, large enough to cover more than one day.
FORECAST_PAST_DAYS = 2
FORECAST_DAYS = 1
ARCHIVE_DAYS = 3

HOURLY_PARAM = ",".join(HOURLY_VARIABLES)


def _user_agent() -> str:
    return f"adcp-fixture-recorder/{__version__} (+https://github.com/depduris/adcp)"


def _get(client: httpx.Client, url: str, params: dict[str, Any]) -> httpx.Response:
    response = client.get(url, params=params)
    response.raise_for_status()
    return response


def record(client: httpx.Client) -> dict[str, Any]:
    """Fetch the live payloads and return them keyed by fixture file name."""
    common: dict[str, Any] = {
        "latitude": LATITUDE,
        "longitude": LONGITUDE,
        "hourly": HOURLY_PARAM,
        "timezone": "UTC",
        "cell_selection": "nearest",
    }

    forecast = _get(
        client,
        FORECAST_URL,
        {
            **common,
            "past_days": FORECAST_PAST_DAYS,
            "forecast_days": FORECAST_DAYS,
        },
    ).json()

    end = datetime.now(UTC).date() - timedelta(days=7)
    start = end - timedelta(days=ARCHIVE_DAYS - 1)
    archive = _get(
        client,
        ARCHIVE_URL,
        {
            **common,
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
        },
    ).json()

    # A deliberately invalid request: Open-Meteo answers 400 with an error document.
    invalid = client.get(FORECAST_URL, params={**common, "latitude": 999.0})
    error_document = {
        "http_status": invalid.status_code,
        "body": invalid.json(),
    }

    return {
        "forecast_single_location.json": forecast,
        "archive_date_range.json": archive,
        "error_invalid_coordinates.json": error_document,
    }


def derive_anomalies(valid: dict[str, Any]) -> dict[str, Any]:
    """Derive every anomaly fixture from the recorded payload."""
    hourly = valid["hourly"]
    units = valid["hourly_units"]

    ragged = copy.deepcopy(valid)
    ragged["hourly"]["temperature_2m"] = ragged["hourly"]["temperature_2m"][:-1]

    missing_variable = copy.deepcopy(valid)
    del missing_variable["hourly"]["wind_gusts_10m"]

    missing_hourly = copy.deepcopy(valid)
    del missing_hourly["hourly"]

    units_changed = copy.deepcopy(valid)
    units_changed["hourly_units"]["wind_speed_10m"] = "m/s"

    wrong_type = copy.deepcopy(valid)
    wrong_type["hourly"]["temperature_2m"][0] = "17.4"

    invalid_timestamp = copy.deepcopy(valid)
    invalid_timestamp["hourly"]["time"][0] = "not-a-timestamp"

    non_monotonic = copy.deepcopy(valid)
    non_monotonic["hourly"]["time"][1], non_monotonic["hourly"]["time"][2] = (
        non_monotonic["hourly"]["time"][2],
        non_monotonic["hourly"]["time"][1],
    )

    invalid_location_metadata = copy.deepcopy(valid)
    invalid_location_metadata["latitude"] = 999.0

    sparse = copy.deepcopy(valid)
    sparse["hourly"]["precipitation"][0] = None
    sparse["hourly"]["snowfall"][0] = None

    # A provider-level error document served with 200 OK.
    provider_error = {
        "error": True,
        "reason": "Minutely API request limit exceeded. Please try again in 60 seconds.",
    }

    rate_limited = {
        "http_status": 429,
        "headers": {"retry-after": "42"},
        "body": {
            "error": True,
            "reason": "Minutely API request limit exceeded. Please try again in 60 seconds.",
        },
    }

    print(f"  hourly.time[0] = {hourly['time'][0]!r}", file=sys.stderr)
    print(f"  utc_offset_seconds = {valid.get('utc_offset_seconds')!r}", file=sys.stderr)
    print(f"  units = {json.dumps(units, ensure_ascii=False)}", file=sys.stderr)
    print(f"  hours = {len(hourly['time'])}", file=sys.stderr)

    return {
        "forecast_ragged_arrays.json": ragged,
        "forecast_missing_variable.json": missing_variable,
        "forecast_missing_hourly.json": missing_hourly,
        "forecast_units_changed.json": units_changed,
        "forecast_wrong_field_type.json": wrong_type,
        "forecast_invalid_timestamp.json": invalid_timestamp,
        "forecast_non_monotonic.json": non_monotonic,
        "forecast_invalid_location_metadata.json": invalid_location_metadata,
        "forecast_with_nulls.json": sparse,
        "error_provider_error_document.json": provider_error,
        "error_rate_limited.json": rate_limited,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Record Open-Meteo fixtures.")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("tests/fixtures/open_meteo"),
        help="Directory to write fixtures into.",
    )
    arguments = parser.parse_args()
    arguments.output_dir.mkdir(parents=True, exist_ok=True)

    headers = {"User-Agent": _user_agent(), "Accept": "application/json"}
    with httpx.Client(headers=headers, timeout=30.0, follow_redirects=True) as client:
        recorded = record(client)

    valid_forecast = recorded["forecast_single_location.json"]
    fixtures = {**recorded, **derive_anomalies(valid_forecast)}

    for name, payload in fixtures.items():
        path = arguments.output_dir / name
        path.write_text(
            json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        print(f"wrote {path}", file=sys.stderr)

    # A body that is not JSON at all, kept as raw text so the test exercises the
    # malformed-JSON path rather than the schema path.
    malformed = arguments.output_dir / "error_malformed_json.json"
    malformed.write_text(
        '{"latitude": 44.8125, "hourly": {"time": ["2026-01-0',
        encoding="utf-8",
    )
    print(f"wrote {malformed}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
