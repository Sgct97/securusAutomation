"""
Oklahoma DOC Bulk Data Parser

Parses fixed-width .dat files from the OK DOC vendor extract.
Filters by LAST_MOVE_DATE for recently received inmates and
loads them into the database.

Data source: vendor extract 01/Vendor_Profile_Extract_Text.dat
Format docs: vendor extract 01/ReadMe.txt
"""

import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from sqlalchemy import select
from sqlalchemy.dialects.sqlite import insert as sqlite_upsert

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config import settings
from database import Inmate, InmateStatus, init_db, async_session_factory
from logger import get_logger

log = get_logger("scraper.oklahoma")

DATA_DIR = Path(__file__).resolve().parent.parent / "vendor extract 01"
PROFILE_FILE = DATA_DIR / "Vendor_Profile_Extract_Text.dat"

PROFILE_FIELDS = [
    ("DOC_NUM", 10),
    ("LAST_NAME", 30),
    ("FIRST_NAME", 30),
    ("MIDDLE_NAME", 30),
    ("SUFFIX", 4),
    ("LAST_MOVE_DATE", 8),
    ("FACILITY", 50),
    ("BIRTH_DATE", 8),
    ("SEX", 1),
    ("RACE", 60),
    ("HAIR", 60),
    ("HEIGHT_FT", 1),
    ("HEIGHT_IN", 2),
    ("WEIGHT", 3),
    ("EYE", 60),
    ("STATUS", 10),
]

LINE_WIDTH = sum(w for _, w in PROFILE_FIELDS)  # 367


def parse_profile_line(line: str) -> dict:
    """Parse a single fixed-width profile record into a dict."""
    rec = {}
    pos = 0
    for name, width in PROFILE_FIELDS:
        rec[name] = line[pos:pos + width].strip()
        pos += width
    return rec


def parse_date(date_str: str) -> Optional[datetime]:
    """Parse YYYYMMDD date string, returning None for blanks/invalid."""
    if not date_str or len(date_str) != 8 or not date_str.isdigit():
        return None
    try:
        return datetime.strptime(date_str, "%Y%m%d")
    except ValueError:
        return None


def build_name(rec: dict) -> str:
    """Build full name from record fields."""
    parts = [rec["FIRST_NAME"], rec["MIDDLE_NAME"], rec["LAST_NAME"]]
    if rec["SUFFIX"]:
        parts.append(rec["SUFFIX"])
    return " ".join(p for p in parts if p)


def iter_profiles(
    min_move_date: Optional[datetime] = None,
    status_filter: str = "ACTIVE",
) -> list[dict]:
    """
    Read all profiles from the bulk data file.

    Args:
        min_move_date: Only include inmates moved on or after this date
        status_filter: Only include inmates with this status (default ACTIVE)

    Returns:
        List of parsed profile dicts matching the filters
    """
    if not PROFILE_FILE.exists():
        raise FileNotFoundError(f"Profile data not found: {PROFILE_FILE}")

    results = []
    total = 0
    skipped_status = 0
    skipped_date = 0

    log.info("Parsing OK profile data", path=str(PROFILE_FILE))

    with open(PROFILE_FILE, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.rstrip("\r\n")
            if len(line) < LINE_WIDTH:
                continue

            total += 1
            rec = parse_profile_line(line)

            if status_filter and rec["STATUS"] != status_filter:
                skipped_status += 1
                continue

            if min_move_date:
                move_dt = parse_date(rec["LAST_MOVE_DATE"])
                if not move_dt or move_dt < min_move_date:
                    skipped_date += 1
                    continue

            results.append(rec)

            if total % 100_000 == 0:
                log.info("Parsing progress", total=total, matched=len(results))

    log.info("Parsing complete",
             total_records=total,
             matched=len(results),
             skipped_status=skipped_status,
             skipped_date=skipped_date)
    return results


async def load_to_database(
    profiles: list[dict],
    batch_size: int = 500,
) -> tuple[int, int]:
    """
    Load parsed profiles into the Inmate table.
    Uses upsert to handle duplicates (same inmate_id + state).

    Returns:
        (inserted_count, updated_count)
    """
    await init_db()

    inserted = 0
    updated = 0

    async with async_session_factory() as session:
        for i in range(0, len(profiles), batch_size):
            batch = profiles[i:i + batch_size]

            for rec in batch:
                name = build_name(rec)
                move_date = parse_date(rec["LAST_MOVE_DATE"])

                existing = await session.execute(
                    select(Inmate).where(
                        Inmate.inmate_id == rec["DOC_NUM"],
                        Inmate.state == "OK",
                    )
                )
                existing_inmate = existing.scalar_one_or_none()

                if existing_inmate:
                    existing_inmate.name = name
                    existing_inmate.facility = rec["FACILITY"]
                    existing_inmate.status = InmateStatus.ACTIVE.value
                    existing_inmate.last_verified = datetime.now(timezone.utc)
                    updated += 1
                else:
                    inmate = Inmate(
                        inmate_id=rec["DOC_NUM"],
                        name=name,
                        state="OK",
                        facility=rec["FACILITY"],
                        status=InmateStatus.ACTIVE.value,
                        source_url="ok_doc_bulk_extract",
                        discovered_at=move_date or datetime.now(timezone.utc),
                    )
                    session.add(inmate)
                    inserted += 1

            await session.commit()
            log.info("Batch committed",
                     batch=i // batch_size + 1,
                     inserted=inserted,
                     updated=updated)

    log.info("Database load complete", inserted=inserted, updated=updated)
    return inserted, updated


async def run(
    days_back: int = 90,
    status: str = "ACTIVE",
):
    """
    Main entry point: parse OK bulk data and load recent inmates to DB.

    Args:
        days_back: How far back to look for LAST_MOVE_DATE (default 90 days)
        status: Filter by inmate status (default ACTIVE)
    """
    cutoff = datetime.now() - timedelta(days=days_back)
    log.info("Oklahoma parser starting",
             cutoff_date=cutoff.strftime("%Y-%m-%d"),
             status_filter=status)

    profiles = iter_profiles(min_move_date=cutoff, status_filter=status)
    log.info("Profiles to load", count=len(profiles))

    if not profiles:
        log.warning("No profiles matched filters")
        return 0, 0

    inserted, updated = await load_to_database(profiles)
    return inserted, updated


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Parse OK DOC bulk data")
    parser.add_argument("--days", type=int, default=90,
                        help="How many days back to look (default 90)")
    parser.add_argument("--status", default="ACTIVE",
                        help="Status filter (default ACTIVE)")
    parser.add_argument("--all-active", action="store_true",
                        help="Load ALL active inmates regardless of date")
    args = parser.parse_args()

    if args.all_active:
        async def run_all():
            profiles = iter_profiles(status_filter=args.status)
            return await load_to_database(profiles)
        inserted, updated = asyncio.run(run_all())
    else:
        inserted, updated = asyncio.run(run(days_back=args.days, status=args.status))

    print(f"\nDone: {inserted} inserted, {updated} updated")
