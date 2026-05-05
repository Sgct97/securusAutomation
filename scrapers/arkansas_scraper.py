"""
Arkansas DOC scraper - ADC number enumeration via the new
inmate.ark.org Laravel-backed search form.

Site as of 2026-05:
  - Home: https://inmate.ark.org/
  - Search: GET https://inmate.ark.org/index.php/results
  - Submitting only an ADC number redirects (302) to a detail
    URL like /index.php/<adc>-<internal_id>; missing inmates leave
    the request on /index.php/results with no detail body.

Form requirements learned from a real-browser submission:
  - `_token` (CSRF) hidden input must come from a fresh GET of /
  - Empty selects must be submitted as `0`, NOT empty string
    (Laravel `in:` validation rejects `""`)
  - `age_type` MUST be "1" (the default), not empty
  - `sex` is omitted entirely when no radio is selected
  - `disclaimer` must be `1`
  - The request is a GET (not POST) and follows the redirect

ADC numbers (unchanged from old site):
  - Males:   ~069000-190000+
  - Females: ~600000-760000+

Strategy for finding NEW inmates:
  1. Resume from the highest ADC we already have per gender series.
  2. Enumerate upward from there.
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
from database import (
    Inmate,
    OutreachRecord,
    OutreachStatus,
    async_session_factory,
    engine,
    Base,
)

log = structlog.get_logger()

AR_HOME_URL = "https://inmate.ark.org/"
AR_SEARCH_URL = "https://inmate.ark.org/index.php/results"

BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,*/*;q=0.8"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Upgrade-Insecure-Requests": "1",
}


async def get_token(client: httpx.AsyncClient) -> str:
    """Fetch home and extract the Laravel CSRF token."""
    resp = await client.get(AR_HOME_URL, timeout=15)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    token_input = soup.select_one("input[name='_token']")
    if not token_input or not token_input.get("value"):
        raise RuntimeError("Could not find _token on AR home page")
    return token_input["value"]


def is_detail_url(url: str) -> bool:
    """A detail URL looks like /index.php/<adc>-<internal_id>."""
    # Match /index.php/123456-7890123 (digits, dash, digits)
    return bool(re.search(r"/index\.php/\d+-\d+", url))


def parse_detail_page(html: str) -> Optional[dict]:
    """Parse the new inmate detail page.

    The new layout uses Bootstrap-style label/value div pairs but the
    flattened text-line representation alternates LABEL\nVALUE for
    every field we care about, so we keep that simpler approach.
    Returns None if no parseable record was found.
    """
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(separator="\n", strip=True)
    lines = [ln.strip() for ln in text.split("\n") if ln.strip()]

    data: dict = {}
    for i, line in enumerate(lines):
        nxt = lines[i + 1] if i + 1 < len(lines) else ""

        if line == "ADC Number" and nxt:
            data["adc_number"] = nxt
        elif line == "Name:" and nxt:
            data["name"] = nxt
        elif line == "Race" and nxt and nxt != "Race":
            data["race"] = nxt
        elif line == "Sex" and nxt in ("Male", "Female"):
            data["gender"] = nxt
        elif line == "Birth Date" and nxt:
            try:
                data["birth_date"] = datetime.strptime(nxt, "%m/%d/%Y")
            except ValueError:
                pass
        elif line == "Initial Receipt Date" and nxt:
            try:
                data["receipt_date"] = datetime.strptime(
                    nxt, "%m/%d/%Y"
                )
            except ValueError:
                pass
        elif line == "Facility" and nxt:
            if nxt not in ("Facility Address", "N/A", ""):
                data["facility"] = nxt
        elif line.startswith("PE/TE Date") and nxt:
            try:
                data["release_date"] = datetime.strptime(
                    nxt, "%m/%d/%Y"
                )
            except ValueError:
                pass

    if "adc_number" not in data or "name" not in data:
        return None
    return data


def build_search_params(token: str, adc_num: str) -> dict:
    """Mirror exactly what a real browser submits when only the
    ADC number is filled in (verified via Playwright capture)."""
    return {
        "_token": token,
        "dc_num": adc_num,
        "county": "0",
        "last_name": "",
        "first_name": "",
        "facility": "0",
        "crime": "0",
        "age_type": "1",
        "age": "",
        "ethnicity": "0",
        "disclaimer": "1",
        "B1": "Search",
    }


async def search_adc_number(
    client: httpx.AsyncClient,
    adc_num: str,
    token: str,
) -> Optional[dict]:
    """Search for a single ADC number. Returns inmate dict or None.
    Retries on transient network errors.
    """
    for attempt in range(3):
        try:
            params = build_search_params(token, adc_num)
            resp = await client.get(
                AR_SEARCH_URL,
                params=params,
                headers={"Referer": AR_HOME_URL},
                timeout=15,
                follow_redirects=True,
            )
            if resp.status_code != 200:
                log.warning(
                    "Non-200 response",
                    adc=adc_num,
                    status=resp.status_code,
                )
                return None

            final_url = str(resp.url)
            if not is_detail_url(final_url):
                # Stayed on /results or got bounced to /, no inmate.
                return None

            return parse_detail_page(resp.text)

        except (
            httpx.ReadError,
            httpx.ConnectError,
            httpx.TimeoutException,
        ) as e:
            if attempt < 2:
                wait = (attempt + 1) * 5
                log.warning(
                    "Network error, retrying",
                    adc=adc_num,
                    error=type(e).__name__,
                    wait=wait,
                )
                await asyncio.sleep(wait)
            else:
                log.error(
                    "Network error after 3 attempts",
                    adc=adc_num,
                    error=str(e),
                )
                raise


async def load_inmate_to_db(inmate: dict) -> bool:
    """Upsert a single inmate record. Returns True if newly inserted."""
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
            source_url=AR_HOME_URL,
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
    """Enumerate ADC numbers sequentially.

    Refreshes the CSRF token every 200 requests because Laravel
    rotates session tokens periodically.
    Returns (found_list, latest_token).
    """
    found: list[dict] = []
    consecutive_misses = 0
    current = start
    total_checked = 0
    new_count = 0

    while consecutive_misses < max_consecutive_misses:
        adc = str(current).zfill(6)

        if total_checked > 0 and total_checked % 200 == 0:
            try:
                token = await get_token(client)
            except Exception as e:
                log.warning(
                    "Token refresh failed, continuing with old token",
                    error=str(e),
                )

        result = await search_adc_number(client, adc, token)
        total_checked += 1

        if result:
            consecutive_misses = 0
            is_new = await load_inmate_to_db(result)
            found.append(result)
            if is_new:
                new_count += 1
            receipt = result.get("receipt_date")
            receipt_str = (
                receipt.strftime("%Y-%m-%d") if receipt else "?"
            )
            if total_checked <= 30 or is_new:
                log.info(
                    f"[{label}] {'NEW' if is_new else 'known'}",
                    adc=adc,
                    name=result["name"],
                    facility=result.get("facility", "?"),
                    admitted=receipt_str,
                )
        else:
            consecutive_misses += 1

        current += direction
        await asyncio.sleep(0.15)

        if total_checked % 100 == 0:
            log.info(
                f"[{label}] Progress",
                checked=total_checked,
                found=len(found),
                new=new_count,
                current=current,
                consec_misses=consecutive_misses,
            )

    log.info(
        f"[{label}] Done",
        checked=total_checked,
        found=len(found),
        new=new_count,
        stopped_at=current,
    )
    return found, token


async def get_highest_known_adc(
    series_min: int, series_max: int
) -> Optional[int]:
    """Return the highest numeric ADC in the DB within a range."""
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
            try:
                return int(max_id)
            except ValueError:
                return None
    return None


async def run(
    male_start: Optional[int] = None,
    female_start: Optional[int] = None,
    max_misses: int = 50,
    max_pages: Optional[int] = None,
):
    """Main scraper entry point.

    Args:
        male_start: Starting ADC number for male enumeration
        female_start: Starting ADC number for female enumeration
        max_misses: Stop a series after this many consecutive misses
        max_pages: Unused (kept for signature parity with other scrapers)
    """
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with httpx.AsyncClient(
        headers=BROWSER_HEADERS,
        follow_redirects=True,
    ) as client:
        token = await get_token(client)
        log.info("Got initial CSRF token from inmate.ark.org")
        all_found: list[dict] = []

        # Male series (1xxxxx — current frontier ~186000-190000)
        if male_start is None:
            known_max = await get_highest_known_adc(100000, 299999)
            if known_max:
                male_start = known_max + 1
                log.info(
                    "Resuming male enumeration from DB",
                    start=male_start,
                )
            else:
                male_start = 186000
                log.info(
                    "Starting male enumeration from default",
                    start=male_start,
                )

        males, token = await enumerate_range(
            client,
            male_start,
            token,
            max_consecutive_misses=max_misses,
            direction=1,
            label="MALE",
        )
        all_found.extend(males)

        # Female series (7xxxxx — current frontier ~755000-760000)
        if female_start is None:
            known_max = await get_highest_known_adc(600000, 799999)
            if known_max:
                female_start = known_max + 1
                log.info(
                    "Resuming female enumeration from DB",
                    start=female_start,
                )
            else:
                female_start = 755000
                log.info(
                    "Starting female enumeration from default",
                    start=female_start,
                )

        females, token = await enumerate_range(
            client,
            female_start,
            token,
            max_consecutive_misses=max_misses,
            direction=1,
            label="FEMALE",
        )
        all_found.extend(females)

        log.info(
            "Arkansas scrape complete",
            total=len(all_found),
            males=len(males),
            females=len(females),
        )

    return all_found


if __name__ == "__main__":
    asyncio.run(run())
