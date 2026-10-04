"""Seed the development database with the demo locations.

Idempotent: existing slugs are left untouched, so it is safe to re-run.

    python scripts/seed_locations.py
"""

from __future__ import annotations

import sys
from decimal import Decimal

from adcp.config import get_settings
from adcp.db.engine import create_engine_from_settings, mask_engine_url
from adcp.db.repository import LocationRepository

#: A deliberately varied set: maritime, continental, high latitude, and southern
#: hemisphere, so the collected data is interesting to look at.
LOCATIONS: tuple[dict[str, object], ...] = (
    {
        "slug": "belgrade-rs",
        "name": "Belgrade",
        "latitude": Decimal("44.812500"),
        "longitude": Decimal("20.437500"),
        "country_code": "RS",
    },
    {
        "slug": "reykjavik-is",
        "name": "Reykjavik",
        "latitude": Decimal("64.146600"),
        "longitude": Decimal("-21.942600"),
        "country_code": "IS",
    },
    {
        "slug": "ushuaia-ar",
        "name": "Ushuaia",
        "latitude": Decimal("-54.801900"),
        "longitude": Decimal("-68.302200"),
        "country_code": "AR",
    },
)


def main() -> int:
    settings = get_settings()
    engine = create_engine_from_settings(settings)
    repository = LocationRepository(engine)
    try:
        print(f"seeding {mask_engine_url(engine)}", file=sys.stderr)
        for location in LOCATIONS:
            slug = str(location["slug"])
            if repository.get_by_slug(slug) is not None:
                print(f"  {slug}: already present", file=sys.stderr)
                continue
            created = repository.create(**location)  # type: ignore[arg-type]
            print(f"  {slug}: created (id={created.id})", file=sys.stderr)
    finally:
        engine.dispose()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
