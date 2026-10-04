"""The error taxonomy maps onto the documented exit codes."""

from __future__ import annotations

import pytest

from adcp.errors import (
    AdcpError,
    DatabaseError,
    DatabaseUnavailableError,
    LocationNotFoundError,
    MigrationError,
)

pytestmark = pytest.mark.unit


@pytest.mark.parametrize(
    "error_type",
    [DatabaseError, DatabaseUnavailableError, MigrationError, LocationNotFoundError],
)
def test_every_specific_error_is_an_adcp_error(error_type: type[AdcpError]) -> None:
    assert issubclass(error_type, AdcpError)


def test_database_errors_share_a_base_class() -> None:
    assert issubclass(DatabaseUnavailableError, DatabaseError)
    assert issubclass(MigrationError, DatabaseError)
    assert not issubclass(LocationNotFoundError, DatabaseError)
