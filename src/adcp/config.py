"""Typed, validated application configuration.

Configuration is read from (highest precedence first):

1. explicit constructor arguments (used by tests and the CLI),
2. process environment variables prefixed with ``ADCP_``,
3. a ``.env`` file in the working directory,
4. the defaults declared here.

Everything is validated at startup so a misconfigured process fails fast with a
readable message instead of failing halfway through a run. See ``docs/PLAN.md``
section 15 for the full reference.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Annotated, Any, Literal
from urllib.parse import urlsplit, urlunsplit
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import (
    AliasChoices,
    AnyHttpUrl,
    Field,
    PostgresDsn,
    SecretStr,
    field_validator,
    model_validator,
)
from pydantic_settings import BaseSettings, SettingsConfigDict

Environment = Literal["local", "dev", "staging", "prod"]
LogLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
LogFormat = Literal["json", "console", "auto"]

#: Matches the Compose stack in ``docker-compose.yml``: the published host port is
#: deliberately unusual so the dev database never clashes with a local PostgreSQL.
DEFAULT_DATABASE_URL = "postgresql+psycopg://adcp:adcp_local_dev@localhost:55432/adcp"
MAX_LOOKBACK_HOURS = 92 * 24
MAX_OVERLAP_HOURS = 7 * 24


class Settings(BaseSettings):
    """Resolved application settings.

    Instantiate through :func:`get_settings` in application code, and directly
    (with ``_env_file=None``) in tests so a developer's local ``.env`` cannot
    change test outcomes.
    """

    model_config = SettingsConfigDict(
        env_prefix="ADCP_",
        env_file=".env",
        env_file_encoding="utf-8",
        env_nested_delimiter="__",
        case_sensitive=False,
        extra="ignore",
        validate_by_name=True,
        validate_by_alias=True,
    )

    # -- application ---------------------------------------------------------
    env: Annotated[Environment, Field(description="Deployment environment name.")] = "local"
    service_name: Annotated[str, Field(min_length=1, max_length=64)] = "adcp"
    run_timeout_s: Annotated[int, Field(gt=0, le=86_400)] = 3_600
    healthcheck_freshness_hours: Annotated[int, Field(gt=0, le=720)] = 3

    # -- logging -------------------------------------------------------------
    log_level: LogLevel = "INFO"
    log_format: LogFormat = "console"
    log_include_caller: bool = False

    # -- database ------------------------------------------------------------
    database_url: Annotated[
        PostgresDsn,
        Field(validation_alias=AliasChoices("ADCP_DATABASE_URL", "DATABASE_URL")),
    ] = PostgresDsn(DEFAULT_DATABASE_URL)
    db_pool_min_size: Annotated[int, Field(ge=0, le=100)] = 1
    db_pool_max_size: Annotated[int, Field(ge=1, le=100)] = 5
    db_connect_timeout_s: Annotated[int, Field(gt=0, le=120)] = 5
    db_statement_timeout_ms: Annotated[int, Field(gt=0, le=3_600_000)] = 30_000
    db_slow_query_ms: Annotated[int, Field(gt=0, le=3_600_000)] = 1_000

    # -- upstream API --------------------------------------------------------
    open_meteo_forecast_url: AnyHttpUrl = AnyHttpUrl(
        "https://api.open-meteo.com/v1/forecast",
    )
    open_meteo_archive_url: AnyHttpUrl = AnyHttpUrl(
        "https://archive-api.open-meteo.com/v1/archive",
    )
    open_meteo_historical_forecast_url: AnyHttpUrl = AnyHttpUrl(
        "https://historical-forecast-api.open-meteo.com/v1/forecast",
    )
    open_meteo_timeout_connect_s: Annotated[float, Field(gt=0, le=60)] = 5.0
    open_meteo_timeout_read_s: Annotated[float, Field(gt=0, le=300)] = 20.0
    open_meteo_timeout_write_s: Annotated[float, Field(gt=0, le=300)] = 10.0
    open_meteo_timeout_total_s: Annotated[float, Field(gt=0, le=600)] = 60.0
    open_meteo_max_attempts: Annotated[int, Field(ge=1, le=10)] = 5
    open_meteo_backoff_initial_s: Annotated[float, Field(gt=0, le=300)] = 1.0
    open_meteo_backoff_max_s: Annotated[float, Field(gt=0, le=600)] = 30.0
    open_meteo_max_concurrency: Annotated[int, Field(ge=1, le=32)] = 4
    open_meteo_chunk_pause_s: Annotated[float, Field(ge=0, le=60)] = 0.5
    open_meteo_user_agent: Annotated[str, Field(min_length=1, max_length=256)] = (
        "adcp/0.1.0 (+https://github.com/depduris/adcp)"
    )
    open_meteo_api_key: SecretStr | None = None

    # -- ingestion behaviour --------------------------------------------------
    ingest_lookback_hours: Annotated[int, Field(ge=1, le=MAX_LOOKBACK_HOURS)] = 72
    ingest_overlap_hours: Annotated[int, Field(ge=0, le=MAX_OVERLAP_HOURS)] = 24
    ingest_failure_budget_ratio: Annotated[float, Field(ge=0.0, le=1.0)] = 0.5
    ingest_max_invalid_row_ratio: Annotated[float, Field(ge=0.0, le=1.0)] = 0.25
    locations_file: Path | None = None

    # -- scheduler ------------------------------------------------------------
    scheduler_enabled: bool = False
    scheduler_minute: Annotated[int, Field(ge=0, le=59)] = 7
    scheduler_timezone: str = "UTC"
    scheduler_skip_if_running: bool = True
    # Development/demo cadence. ``0`` means "use the hourly cron model above";
    # any positive value schedules a collection every N seconds instead.
    scheduler_interval_seconds: Annotated[int, Field(ge=0, le=86_400)] = 0

    @field_validator("log_level", mode="before")
    @classmethod
    def _normalise_log_level(cls, value: Any) -> Any:
        """Accept ``debug`` as well as ``DEBUG``.

        Environment configuration is typed by hand often enough that rejecting a
        lower-case level would be a needless source of failed deploys.
        """
        return value.upper() if isinstance(value, str) else value

    @field_validator("env", "log_format", mode="before")
    @classmethod
    def _normalise_lower_case(cls, value: Any) -> Any:
        return value.lower() if isinstance(value, str) else value

    @model_validator(mode="after")
    def _validate_cross_field_rules(self) -> Settings:
        """Enforce invariants that span more than one setting."""
        if self.db_pool_max_size < self.db_pool_min_size:
            msg = (
                f"db_pool_max_size ({self.db_pool_max_size}) must be >= "
                f"db_pool_min_size ({self.db_pool_min_size})"
            )
            raise ValueError(msg)

        if self.open_meteo_backoff_max_s < self.open_meteo_backoff_initial_s:
            msg = (
                f"open_meteo_backoff_max_s ({self.open_meteo_backoff_max_s}) must be >= "
                f"open_meteo_backoff_initial_s ({self.open_meteo_backoff_initial_s})"
            )
            raise ValueError(msg)

        if self.ingest_overlap_hours > self.ingest_lookback_hours:
            msg = (
                f"ingest_overlap_hours ({self.ingest_overlap_hours}) must be <= "
                f"ingest_lookback_hours ({self.ingest_lookback_hours})"
            )
            raise ValueError(msg)

        try:
            ZoneInfo(self.scheduler_timezone)
        except (ZoneInfoNotFoundError, ValueError) as exc:
            msg = f"scheduler_timezone {self.scheduler_timezone!r} is not a known IANA timezone"
            raise ValueError(msg) from exc

        self._validate_production_guards()
        return self

    def _validate_production_guards(self) -> None:
        """Refuse configurations that are safe locally and wrong in production.

        Both cases are silent-default mistakes rather than typos: the shipped
        local DSN and console logs are exactly what a production deployment must
        not inherit (PLAN sections 12.1 and 15.2).
        """
        if self.env != "prod":
            return
        if str(self.database_url) == DEFAULT_DATABASE_URL:
            msg = (
                "ADCP_DATABASE_URL is still the shipped local-development default "
                f"({DEFAULT_DATABASE_URL!r}); set it to the production database"
            )
            raise ValueError(msg)
        if self.log_format == "console":
            msg = "ADCP_LOG_FORMAT must be 'json' (or 'auto') when ADCP_ENV=prod"
            raise ValueError(msg)

    # -- derived helpers -------------------------------------------------------
    @property
    def is_production(self) -> bool:
        """Whether this process is running in the production environment."""
        return self.env == "prod"

    def masked_database_url(self) -> str:
        """Return the database URL with the password replaced by ``***``.

        Safe to log and to print in CLI output. The username, host, port and
        database are preserved so operators can still tell which database is in use.
        """
        return mask_url_credentials(str(self.database_url))

    def safe_dump(self) -> dict[str, Any]:
        """Return every setting as JSON-serialisable data with secrets masked."""
        dumped = self.model_dump(mode="json")
        dumped["database_url"] = self.masked_database_url()
        dumped["open_meteo_api_key"] = "***" if self.open_meteo_api_key else None
        return {key: dumped[key] for key in sorted(dumped)}

    def with_overrides(self, **overrides: Any) -> Settings:
        """Return a re-validated copy with overrides applied.

        Used by the CLI for flags such as ``--lookback-hours``: unlike
        ``model_copy(update=...)`` this runs the validators again, so an override
        that breaks a cross-field rule fails fast with exit code 2.
        """
        merged = {**self.model_dump(), **overrides}
        return type(self).model_validate(merged)


def mask_url_credentials(value: str) -> str:
    """Replace the password component of a URL-like string with ``***``."""
    parts = urlsplit(value)
    if not parts.netloc or "@" not in parts.netloc:
        return value

    credentials, _, host = parts.netloc.rpartition("@")
    user, separator, _password = credentials.partition(":")
    masked_credentials = f"{user}:***" if separator else user
    return urlunsplit(parts._replace(netloc=f"{masked_credentials}@{host}"))


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return the process-wide settings singleton.

    Cached so the environment is read once per process. Tests that manipulate the
    environment must call ``get_settings.cache_clear()`` (the ``settings`` fixture
    in ``tests/conftest.py`` does this automatically).
    """
    return Settings()


__all__ = [
    "DEFAULT_DATABASE_URL",
    "Environment",
    "LogFormat",
    "LogLevel",
    "Settings",
    "get_settings",
    "mask_url_credentials",
]
