"""Tests for the structured logging skeleton."""

from __future__ import annotations

import io
import json

import pytest
import structlog

from adcp.logging import REDACTED, configure_logging, get_logger

pytestmark = pytest.mark.unit


def _lines(buffer: io.StringIO) -> list[str]:
    return [line for line in buffer.getvalue().splitlines() if line.strip()]


def test_json_format_emits_one_parseable_object_per_line() -> None:
    buffer = io.StringIO()
    configure_logging(
        level="INFO",
        log_format="json",
        service="adcp",
        environment="test",
        stream=buffer,
    )

    get_logger("adcp.test").info("ingest.run.started", run_id="abc-123", locations=3)

    lines = _lines(buffer)
    assert len(lines) == 1

    payload = json.loads(lines[0])
    assert payload["event"] == "ingest.run.started"
    assert payload["level"] == "info"
    assert payload["run_id"] == "abc-123"
    assert payload["locations"] == 3
    assert payload["service"] == "adcp"
    assert payload["env"] == "test"
    assert payload["timestamp"].endswith("Z") or "+00:00" in payload["timestamp"]


def test_auto_format_uses_json_when_stream_is_not_a_tty() -> None:
    buffer = io.StringIO()
    configure_logging(log_format="auto", stream=buffer)

    get_logger("adcp.test").info("app.startup")

    payload = json.loads(_lines(buffer)[0])
    assert payload["event"] == "app.startup"


def test_console_format_is_human_readable() -> None:
    buffer = io.StringIO()
    configure_logging(log_format="console", stream=buffer)

    get_logger("adcp.test").info("ingest.location.completed", location="belgrade-rs")

    output = buffer.getvalue()
    assert "ingest.location.completed" in output
    assert "belgrade-rs" in output


def test_log_level_filters_lower_severity_records() -> None:
    buffer = io.StringIO()
    configure_logging(level="WARNING", log_format="json", stream=buffer)
    logger = get_logger("adcp.test")

    logger.info("app.startup")
    logger.warning("ingest.run.skipped")

    lines = _lines(buffer)
    assert len(lines) == 1
    assert json.loads(lines[0])["event"] == "ingest.run.skipped"


@pytest.mark.parametrize(("level", "log_format"), [("LOUD", "json"), ("INFO", "yaml")])
def test_invalid_logging_configuration_is_rejected(level: str, log_format: str) -> None:
    with pytest.raises(ValueError, match="unknown log"):
        configure_logging(level=level, log_format=log_format, stream=io.StringIO())


def test_secrets_are_redacted_everywhere_in_the_record() -> None:
    buffer = io.StringIO()
    configure_logging(log_format="json", stream=buffer)

    get_logger("adcp.test").info(
        "config.loaded",
        database_url="postgresql+psycopg://adcp:topsecret@localhost:5432/adcp",
        open_meteo_api_key="key-123",
        nested={"password": "hunter2", "safe": "visible"},
        endpoint="https://user:token@example.com/api",
    )

    output = buffer.getvalue()
    assert "topsecret" not in output
    assert "hunter2" not in output
    assert "key-123" not in output
    assert "token@" not in output
    assert REDACTED in output
    assert "visible" in output
    assert "https://user:***@example.com/api" in output


def test_bound_context_appears_on_every_record() -> None:
    buffer = io.StringIO()
    configure_logging(log_format="json", stream=buffer)
    structlog.contextvars.bind_contextvars(run_id="run-42", location="belgrade-rs")
    try:
        logger = get_logger("adcp.test")
        logger.info("ingest.location.started")
        logger.info("ingest.location.completed")
    finally:
        structlog.contextvars.clear_contextvars()

    payloads = [json.loads(line) for line in _lines(buffer)]
    assert len(payloads) == 2
    assert all(item["run_id"] == "run-42" for item in payloads)
    assert all(item["location"] == "belgrade-rs" for item in payloads)


def _raise_with_credentials() -> None:
    """Raise an error whose message contains a DSN and an API key."""
    msg = "cannot use postgresql+psycopg://adcp:hunter2@db.internal:5432/adcp?apikey=canary-key"
    raise RuntimeError(msg)


def test_credentials_inside_a_traceback_are_redacted() -> None:
    """Redaction runs after the traceback is rendered into a string."""
    buffer = io.StringIO()
    configure_logging(log_format="json", stream=buffer)
    logger = get_logger("adcp.test")

    try:
        _raise_with_credentials()
    except RuntimeError:
        # G201: structlog's API takes exc_info; the test is about the rendering.
        logger.error("db.pool.failed", exc_info=True)  # noqa: G201

    output = buffer.getvalue()
    assert "hunter2" not in output
    assert "canary-key" not in output
    assert "***" in output
    payload = json.loads(_lines(buffer)[0])
    assert "RuntimeError" in payload["exception"]
