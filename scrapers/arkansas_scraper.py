"""
Arkansas DOC scraper — ADC number enumeration via HTTP POST.

The AR inmate search is a PHP form requiring a CSRF token.
Single-number searches return a detail page with full inmate info
including Initial Receipt Date (admission date).

ADC numbers:
  - Males:   6-digit, range ~069000-190000+ (growing)
  - Females: 6-digit, range ~600000-760000+ (separate series)

Strategy for finding NEW inmates:
  1. Store the highest ADC number seen per gender series.
  2. Each run, enumerate upward from that number.
  3. Stop after N consecutive misses (past the frontier).
"""

import asyncio
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import httpx
from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import structlog
from sqlalchemy import select, func

from config import settings
from database import Inmate, OutreachRecord, OutreachStatus, async_session_factory, engine, Base

log = structlog.get_logger()

AR_INDEX_URL = "https://apps.ark.org/inmate_info/index.php"
AR_SEARCH_URL = "https://apps.ark.org/inmate_info/search.php"


async def get_token(client: httpx.AsyncClient) -> str:
    """Fetch the search page and extract the CSRF token."""
    resp = await client.get(AR_INDEX_URL, timeout=15)
    soup = BeautifulSoup(resp.text, "html.parser")
    token_input = soup.find("input", {"name": "token"})
    if not token_input:
        raise RuntimeError("Could not find CSRF token on AR search page")
    return token_input["value"]


def parse_detail_page(html: str) -> Optional[dict]:
    """Parse the single-inmate detail page."""
    if "0 matches" in html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(separator="\n", strip=True)
    lines = text.split("\n")

    if "0 matches" in text:
        return None

    data = {}
    for i, line in enumerate(lines):
        if line == "ADC Number" and i + 1 < len(lines):
            data["adc_number"] = lines[i + 1].strip()
        elif line == "Name:" and i + 1 < len(lines):
            data["name"] = lines[i + 1].strip()
        elif line == "Race" and i + 1 < len(lines) and "Race" not in lines[i + 1]:
            data["race"] = lines[i + 1].strip()
        elif line == "Sex" and i + 1 < len(lines):
            data["gender"] = lines[i + 1].strip()
        elif line == "Birth Date" and i + 1 < len(lines):
            try:
                data["birth_date"] = datetime.strptime(lines[i + 1].strip(), "%m/%d/%Y")
            except ValueError:
                pass
        elif line == "Initial Receipt Date" and i + 1 < len(lines):
            try:
                data["receipt_date"] = datetime.strptime(lines[i + 1].strip(), "%m/%d/%Y")
            except ValueError:
                pass
        elif line == "Facility" and i + 1 < len(lines):
            fac = lines[i + 1].strip()
            if fac not in ("Facility Address", "N/A", ""):
                data["facility"] = fac
        elif line.startswith("PE/TE Date") and i + 1 < len(lines):
            try:
                data["release_date"] = datetime.strptime(lines[i + 1].strip(), "%m/%d/%Y")
            except ValueError:
                pass

    if "adc_number" not in data or "name" not in data:
        return None

    return data


async def search_adc_number(client: httpx.AsyncClient, adc_num: str, token: str) -> Optional[dict]:
    """Search for a single ADC number. Returns inmate dict or None. Retries on network errors."""
    data = {
        "token": token,
        "dcnum": adc_num,
        "lastname": "",
        "firstname": "",
        "sex": "b",
        "agetype": "1",
        "age": "",
        "disclaimer": "1",
    }

    for attempt in range(3):
        try:
            resp = await client.post(AR_SEARCH_URL, data=data, timeout=15)
            if resp.status_code != 200:
                log.warning("Non-200 response", adc=adc_num, status=resp.status_code)
                return None
            return parse_detail_page(resp.text)
        except (httpx.ReadError, httpx.ConnectError, httpx.TimeoutException) as e:
            if attempt < 2:
                wait = (attempt + 1) * 5
                log.warning("Network error, retrying", adc=adc_num, error=type(e).__name__, wait=wait)
                await asyncio.sleep(wait)
            else:
                log.error("Network error after 3 attempts", adc=adc_num, error=str(e))
                raise


async def load_inmate_to_db(inmate: dict) -> bool:
    """Upsert a single inmate record. Returns True if new."""
    async with async_session_factory() as session:
        result = await session.execute(
            select(Inmate).where(
                Inmate.inmate_id == inmate["adc_number"],
                Inmate.state == "AR",
            )
        )
        existing = result.scalar_one_or_none()

        if existing:
            existing.name = inmate["name"]
            existing.facility = inmate.get("facility")
            existing.release_date = inmate.get("release_date")
            existing.admission_date = inmate.get("receipt_date")
            existing.last_verified = datetime.now(timezone.utc)
            await session.commit()
            return False

        new_inmate = Inmate(
            inmate_id=inmate["adc_number"],
            name=inmate["name"],
            state="AR",
            facility=inmate.get("facility"),
            release_date=inmate.get("release_date"),
            admission_date=inmate.get("receipt_date"),
            status="active",
            source_url=f"{AR_INDEX_URL}",
            discovered_at=datetime.now(timezone.utc),
            last_verified=datetime.now(timezone.utc),
        )
        session.add(new_inmate)
        await session.commit()
        return True


async def enumerate_range(
    client: httpx.AsyncClient,
    start: int,
    token: str,
    max_consecutive_misses: int = 50,
    direction: int = 1,
    label: str = "",
) -> tuple[list[dict], str]:
    """
    Enumerate ADC numbers sequentially.
    Refreshes token every 200 requests to avoid expiry.
    Returns (found_list, latest_token).
    """
    found = []
    consecutive_misses = 0
    current = start
    total_checked = 0
    new_count = 0

    while consecutive_misses < max_consecutive_misses:
        adc = str(current).zfill(6)

        # Refresh token periodically
        if total_checked > 0 and total_checked % 200 == 0:
            try:
                token = await get_token(client)
            except Exception as e:
                log.warning("Token refresh failed, continuing with old token", error=str(e))

        result = await search_adc_number(client, adc, token)
        total_checked += 1

        if result:
            consecutive_misses = 0
            is_new = await load_inmate_to_db(result)
            found.append(result)
            if is_new:
                new_count += 1
            receipt = result.get("receipt_date")
            receipt_str = receipt.strftime("%Y-%m-%d") if receipt else "?"
            if total_checked <= 30 or is_new:
                log.info(f"[{label}] {'NEW' if is_new else 'known'}",
                         adc=adc, name=result["name"],
                         facility=result.get("facility", "?"),
                         admitted=receipt_str)
        else:
            consecutive_misses += 1

        current += direction
        await asyncio.sleep(0.15)

        if total_checked % 100 == 0:
            log.info(f"[{label}] Progress",
                     checked=total_checked, found=len(found),
                     new=new_count, current=current,
                     consec_misses=consecutive_misses)

    log.info(f"[{label}] Done",
             checked=total_checked, found=len(found), new=new_count,
             stopped_at=current)
    return found, token


async def get_highest_known_adc(series_min: int, series_max: int) -> Optional[int]:
    """Get the highest ADC number we have in the DB for a given range."""
    async with async_session_factory() as session:
        result = await session.execute(
            select(func.max(Inmate.inmate_id)).where(
                Inmate.state == "AR",
                Inmate.inmate_id >= str(series_min),
                Inmate.inmate_id <= str(series_max),
            )
        )
        max_id = result.scalar_one_or_none()
        if max_id:
            return int(max_id)
    return None


async def run(
    male_start: Optional[int] = None,
    female_start: Optional[int] = None,
    max_misses: int = 50,
    max_pages: Optional[int] = None,
):
    """
    Main scraper entry point.

    Args:
        male_start: Starting ADC number for male enumeration
        female_start: Starting ADC number for female enumeration
        max_misses: Stop after this many consecutive misses
        max_pages: Not used (interface consistency)
    """
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with httpx.AsyncClient(
        headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"},
        follow_redirects=True,
    ) as client:
        token = await get_token(client)
        log.info("Got initial CSRF token")
        all_found = []

        # Male series (1xxxxx — current range ~180000-190000)
        if male_start is None:
            known_max = await get_highest_known_adc(100000, 299999)
            if known_max:
                male_start = known_max + 1
                log.info("Resuming male enumeration from DB", start=male_start)
            else:
                male_start = 186000
                log.info("Starting male enumeration from default", start=male_start)

        males, token = await enumerate_range(
            client, male_start, token,
            max_consecutive_misses=max_misses,
            direction=1, label="MALE",
        )
        all_found.extend(males)

        # Female series (7xxxxx — current range ~750000-760000)
        if female_start is None:
            known_max = await get_highest_known_adc(600000, 799999)
            if known_max:
                female_start = known_max + 1
                log.info("Resuming female enumeration from DB", start=female_start)
            else:
                female_start = 755000
                log.info("Starting female enumeration from default", start=female_start)

        females, token = await enumerate_range(
            client, female_start, token,
            max_consecutive_misses=max_misses,
            direction=1, label="FEMALE",
        )
        all_found.extend(females)

        log.info("Arkansas scrape complete",
                 total=len(all_found),
                 males=len(males), females=len(females))

    return all_found


if __name__ == "__main__":
    asyncio.run(run())
