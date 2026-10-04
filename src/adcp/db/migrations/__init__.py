"""Alembic migration environment.

``env.py``       - Alembic's runtime entry point (online and offline modes)
``runner.py``    - programmatic API used by ``adcp db ...`` and the test suite
``script.py.mako`` - template for ``alembic revision --autogenerate``
``versions/``    - the migrations themselves, one revision per table

Migration files are frozen historical snapshots and therefore never import
``adcp``: a two-year-old revision must keep describing exactly the DDL it always
described, regardless of how the application code evolves afterwards.
"""

from __future__ import annotations

from pathlib import Path

#: Directory holding ``env.py`` and ``versions/``; used as Alembic's script_location.
MIGRATIONS_DIR = Path(__file__).parent

__all__ = ["MIGRATIONS_DIR"]
