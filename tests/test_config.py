"""Tests for the configuration skeleton."""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

import pytest
from pydantic import ValidationError

from adcp.config import DEFAULT_DATABASE_URL, Settings, get_settings, mask_url_credentials

pytestmark = pytest.mark.unit


def test_defaults_are_local_development_safe() -> None:
    settings = Settings(_env_file=None)

    assert settings.env == "local"
    assert settings.service_name == "adcp"
    assert settings.log_level == "INFO"
    assert settings.log_format == "console"
    assert settings.log_include_caller is False
    assert str(settings.database_url) == DEFAULT_DATABASE_URL
    assert settings.db_pool_min_size == 1
    assert settings.db_pool_max_size == 5
    assert settings.ingest_lookback_hours == 72
    assert settings.ingest_overlap_hours == 24
    assert settings.scheduler_enabled is False
    assert settings.scheduler_minute == 7
    assert settings.scheduler_timezone == "UTC"
    assert settings.scheduler_interval_seconds == 0
    assert settings.scheduler_skip_if_running is True
    assert settings.locations_file is None
    assert settings.open_meteo_api_key is None


def test_environment_variables_override_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ADCP_ENV", "staging")
    monkeypatch.setenv("ADCP_LOG_LEVEL", "debug")  # case-insensitive
    monkeypatch.setenv("ADCP_LOG_FORMAT", "json")
    monkeypatch.setenv("ADCP_INGEST_LOOKBACK_HOURS", "48")
    monkeypatch.setenv("ADCP_SCHEDULER_ENABLED", "true")

    settings = Settings(_env_file=None)

    assert settings.env == "staging"
    assert settings.log_level == "DEBUG"
    assert settings.log_format == "json"
    assert settings.ingest_lookback_hours == 48
    assert settings.scheduler_enabled is True


def test_unprefixed_database_url_alias_is_supported(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql://user:pw@db.internal:5433/warehouse")

    settings = Settings(_env_file=None)

    assert "db.internal:5433" in str(settings.database_url)
    assert settings.masked_database_url() == "postgresql://user:***@db.internal:5433/warehouse"


def test_prefixed_database_url_wins_over_alias(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql://alias:pw@alias-host:5432/alias_db")
    monkeypatch.setenv("ADCP_DATABASE_URL", "postgresql://adcp:pw@adcp-host:5432/adcp_db")

    settings = Settings(_env_file=None)

    assert "adcp-host" in str(settings.database_url)


def test_settings_can_be_loaded_from_a_dotenv_file(tmp_path: Any) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "ADCP_ENV=prod\n"
        "ADCP_LOG_FORMAT=json\n"
        "ADCP_INGEST_OVERLAP_HOURS=12\n"
        "ADCP_DATABASE_URL=postgresql+psycopg://adcp:secret@db.internal:5432/adcp\n",
        encoding="utf-8",
    )

    settings = Settings(_env_file=env_file)

    assert settings.env == "prod"
    assert settings.is_production is True
    assert settings.log_format == "json"
    assert settings.ingest_overlap_hours == 12
    assert "db.internal" in str(settings.database_url)


@pytest.mark.parametrize(
    ("overrides", "expected_fragment"),
    [
        # The shipped local DSN and console logs are silent-default traps in prod.
        ({"env": "prod", "log_format": "json"}, "local-development default"),
        (
            {
                "env": "prod",
                "log_format": "console",
                "database_url": "postgresql+psycopg://adcp:pw@db.internal:5432/adcp",
            },
            "must be 'json'",
        ),
    ],
)
def test_production_guards_reject_unsafe_defaults(
    make_settings: Callable[..., Settings],
    overrides: dict[str, Any],
    expected_fragment: str,
) -> None:
    with pytest.raises(ValidationError) as excinfo:
        make_settings(**overrides)

    assert expected_fragment in str(excinfo.value)


@pytest.mark.parametrize(
    ("overrides", "expected_fragment"),
    [
        ({"log_level": "VERBOSE"}, "log_level"),
        ({"log_format": "yaml"}, "log_format"),
        ({"env": "production"}, "env"),
        ({"database_url": "sqlite:///adcp.db"}, "database_url"),
        ({"open_meteo_max_attempts": 0}, "open_meteo_max_attempts"),
        ({"db_pool_max_size": 0}, "db_pool_max_size"),
        ({"ingest_failure_budget_ratio": 1.5}, "ingest_failure_budget_ratio"),
        ({"scheduler_minute": 60}, "scheduler_minute"),
        ({"scheduler_timezone": "Mars/Olympus_Mons"}, "scheduler_timezone"),
        ({"scheduler_interval_seconds": -1}, "scheduler_interval_seconds"),
        ({"scheduler_interval_seconds": 100_000}, "scheduler_interval_seconds"),
        ({"db_pool_min_size": 10}, "db_pool_max_size"),
        ({"open_meteo_backoff_initial_s": 60, "open_meteo_backoff_max_s": 30}, "backoff"),
        ({"ingest_lookback_hours": 24, "ingest_overlap_hours": 48}, "overlap"),
    ],
)
def test_invalid_configuration_is_rejected(
    make_settings: Callable[..., Settings],
    overrides: dict[str, Any],
    expected_fragment: str,
) -> None:
    with pytest.raises(ValidationError) as excinfo:
        make_settings(**overrides)

    assert expected_fragment in str(excinfo.value)


def test_masked_database_url_never_leaks_the_password() -> None:
    settings = Settings(_env_file=None)

    masked = settings.masked_database_url()

    assert "adcp_local_dev" not in masked
    assert masked == "postgresql+psycopg://adcp:***@localhost:55432/adcp"
    assert str(settings.database_url) not in masked


def test_safe_dump_is_json_serialisable_and_masks_secrets() -> None:
    settings = Settings(
        _env_file=None,
        database_url="postgresql://user:topsecret@host:5432/db",
        open_meteo_api_key="super-secret-key",
    )

    payload = settings.safe_dump()
    rendered = json.dumps(payload)

    assert "topsecret" not in rendered
    assert "super-secret-key" not in rendered
    assert payload["database_url"] == "postgresql://user:***@host:5432/db"
    assert payload["open_meteo_api_key"] == "***"
    assert payload["env"] == "local"
    assert list(payload) == sorted(payload), "keys are sorted for stable CLI output"


def test_get_settings_is_cached() -> None:
    first = get_settings()
    second = get_settings()

    assert first is second

    get_settings.cache_clear()
    assert get_settings() is not first


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("postgresql://user:pw@host:5432/db", "postgresql://user:***@host:5432/db"),
        ("https://token@example.com/api", "https://token@example.com/api"),
        ("plain-string", "plain-string"),
        ("https://example.com/path", "https://example.com/path"),
    ],
)
def test_mask_url_credentials(value: str, expected: str) -> None:
    assert mask_url_credentials(value) == expected
