"""
New York DOCCS Production Scraper

Enumerates DINs (Department Identification Numbers) sequentially
at https://nysdoccslookup.doccs.ny.gov/

DIN format: YYL#### where:
  YY = 2-digit year of reception
  L  = reception center letter (R, A, B, G, etc.)
  #### = zero-padded sequential number

Validated: DINs are strictly sequential with NO gaps.
Strategy: enumerate from a configurable high-water mark upward.
"""

import asyncio
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from playwright.async_api import async_playwright, Page, TimeoutError as PwTimeout
try:
    from playwright_stealth import Stealth
    _USE_NEW_STEALTH = True
except ImportError:
    from playwright_stealth import stealth_async
    _USE_NEW_STEALTH = False
from sqlalchemy import select

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config import settings
from database import (
    Inmate, InmateStatus, ScrapeProgress,
    init_db, async_session_factory,
)
from logger import get_logger

log = get_logger("scraper.newyork")

NY_URL = "https://nysdoccslookup.doccs.ny.gov/"

RECEPTION_CENTERS = ["R", "A", "B", "G"]


def generate_dins(year: int = 26, letter: str = "R",
                  start: int = 1, count: int = 100) -> list[str]:
    """Generate a list of DINs to check."""
    return [f"{year:02d}{letter}{i:04d}" for i in range(start, start + count)]


async def search_din(page: Page, din: str) -> Optional[dict]:
    """
    Search for a single DIN. Returns inmate dict or None if not found.
    Results appear inline (no detail-page click needed).
    """
    await page.goto(NY_URL, wait_until="domcontentloaded", timeout=30000)
    await page.wait_for_timeout(1500)

    inputs = await page.query_selector_all(
        "input:not([type='submit']):not([type='button']):not([type='hidden'])"
    )
    if not inputs:
        log.error("No search input found")
        return None

    await inputs[0].fill(din)
    await page.wait_for_timeout(300)

    btn = await page.query_selector("button[type='submit']")
    if btn:
        await btn.click()
    else:
        await page.keyboard.press("Enter")

    await page.wait_for_load_state("domcontentloaded", timeout=30000)
    await page.wait_for_timeout(1500)

    text = await page.evaluate("() => document.body.innerText")

    if "No inmates found" in text or "no results" in text.lower():
        return None
    if f"DIN: {din}" not in text and din not in text:
        return None

    name = None
    name_match = re.search(r'^([A-Z][A-Z\',\-\.\s]+)\nDIN:\s*' + re.escape(din), text, re.MULTILINE)
    if name_match:
        name = name_match.group(1).strip()

    date_received = None
    date_match = re.search(r'Date Received \(original\):\s*(\d{2}/\d{2}/\d{4})', text)
    if date_match:
        try:
            date_received = datetime.strptime(date_match.group(1), "%m/%d/%Y")
        except ValueError:
            pass

    facility = None
    fac_match = re.search(r'Housing / Releasing Facility:\s*\n([A-Z][A-Z\s\'\-]+)', text)
    if fac_match:
        facility = fac_match.group(1).strip().split('\n')[0].strip()

    custody_status = None
    cs_match = re.search(r'Custody Status:\s*\n?(.+)', text)
    if cs_match:
        custody_status = cs_match.group(1).strip()

    if not name:
        if "DIN:" in text or "Date of Birth" in text:
            log.warning("DIN exists but failed to parse name", din=din)
            name = f"UNKNOWN ({din})"
        else:
            return None

    return {
        "din": din,
        "name": name,
        "facility": facility,
        "date_received": date_received,
        "custody_status": custody_status,
    }


async def save_progress(last_letter: str, last_num: int, total: int, status: str = "running"):
    async with async_session_factory() as session:
        result = await session.execute(
            select(ScrapeProgress).where(ScrapeProgress.state == "NY")
        )
        progress = result.scalar_one_or_none()
        if progress:
            progress.last_letter = last_letter
            progress.last_page = last_num
            progress.total_found = total
            progress.status = status
            progress.last_updated = datetime.now(timezone.utc)
        else:
            progress = ScrapeProgress(
                state="NY", last_letter=last_letter, last_page=last_num,
                total_found=total, status=status,
                started_at=datetime.now(timezone.utc),
            )
            session.add(progress)
        await session.commit()


async def load_inmate_to_db(rec: dict):
    async with async_session_factory() as session:
        existing = await session.execute(
            select(Inmate).where(
                Inmate.inmate_id == rec["din"],
                Inmate.state == "NY",
            )
        )
        if existing.scalar_one_or_none():
            return False

        inmate = Inmate(
            inmate_id=rec["din"],
            name=rec["name"],
            state="NY",
            facility=rec.get("facility"),
            status=InmateStatus.ACTIVE.value,
            source_url=NY_URL,
            discovered_at=datetime.now(timezone.utc),
            admission_date=rec.get("date_received"),
        )
        session.add(inmate)
        await session.commit()
        return True


async def run(
    year: int = 26,
    letter: str = "R",
    start_num: int = 1,
    max_count: int = 50,
    stop_after_misses: int = 5,
):
    """
    Enumerate DINs and scrape inmate data.

    Args:
        year: 2-digit reception year (26 = 2026)
        letter: Reception center letter
        start_num: Starting sequence number
        max_count: Max DINs to check
        stop_after_misses: Stop after this many consecutive not-found results
    """
    await init_db()

    log.info("New York DOCCS scraper starting",
             year=year, letter=letter, start=start_num, max_count=max_count)

    found_total = 0
    consecutive_misses = 0

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=settings.headless,
            args=["--no-sandbox", "--disable-blink-features=AutomationControlled"],
        )
        context = await browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/131.0.0.0 Safari/537.36"
            ),
        )
        page = await context.new_page()
        if _USE_NEW_STEALTH:
            stealth = Stealth()
            await stealth.apply_stealth_async(page)
        else:
            await stealth_async(page)
        page.set_default_timeout(15000)

        for i in range(start_num, start_num + max_count):
            din = f"{year:02d}{letter}{i:04d}"
            log.info("Searching DIN", din=din)

            try:
                result = await search_din(page, din)

                if result:
                    log.info("Found inmate",
                             din=din, name=result["name"],
                             facility=result.get("facility"))
                    is_new = await load_inmate_to_db(result)
                    if is_new:
                        found_total += 1
                    consecutive_misses = 0
                else:
                    log.info("DIN not found", din=din)
                    consecutive_misses += 1

                    if consecutive_misses >= stop_after_misses:
                        log.info("Reached end of sequence",
                                 misses=consecutive_misses,
                                 last_din=din)
                        break

            except Exception as e:
                log.error("Error searching DIN", din=din, error=str(e))
                consecutive_misses += 1

            await save_progress(letter, i, found_total)

            # Rate limit
            delay = settings.scraper_request_delay
            await page.wait_for_timeout(int(delay * 1000))

        await browser.close()

    await save_progress(letter, i, found_total, status="completed")
    log.info("New York scraper complete",
             found=found_total, last_sequence=i)
    return found_total


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Scrape NY DOCCS by DIN enumeration")
    parser.add_argument("--year", type=int, default=26, help="2-digit year (default: 26)")
    parser.add_argument("--letter", default="R", help="Center letter (default: R)")
    parser.add_argument("--start", type=int, default=1, help="Start sequence (default: 1)")
    parser.add_argument("--count", type=int, default=50, help="Max DINs to check (default: 50)")
    parser.add_argument("--misses", type=int, default=5,
                        help="Stop after N consecutive misses (default: 5)")
    args = parser.parse_args()

    asyncio.run(run(
        year=args.year, letter=args.letter,
        start_num=args.start, max_count=args.count,
        stop_after_misses=args.misses,
    ))
