"""
California CDCR Production Scraper

Enumerates CDCR numbers at https://ciris.mt.cdcr.ca.gov/search
Must handle: SSL cert errors, terms popup, agree button, CDCR Number radio.

CDCR# format:
  Men:   CC##### (current prefix as of late 2025/early 2026)
  Women: WH#####

Validated: ~15-20% gap rate — scraper handles not-found gracefully.
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

log = get_logger("scraper.california")

CA_URL = "https://ciris.mt.cdcr.ca.gov/search"


async def dismiss_popup_and_agree(page: Page):
    """Dismiss the Vuetify overlay popup, then click Agree on disclaimer."""
    close_btn = await page.query_selector('button:has-text("Close")')
    if close_btn and await close_btn.is_visible():
        await close_btn.click()
        await page.wait_for_timeout(1000)

    agree_btn = await page.query_selector('button:has-text("agree")')
    if agree_btn and await agree_btn.is_visible():
        await agree_btn.click()
        await page.wait_for_timeout(2000)


async def select_cdcr_radio(page: Page) -> bool:
    """Click the 'CDCR Number' radio/label on the search form."""
    label = await page.query_selector('label:has-text("CDCR Number")')
    if label:
        await label.click()
        await page.wait_for_timeout(500)
        return True
    return False


async def search_cdcr(page: Page, cdcr_num: str) -> Optional[dict]:
    """Search for a single CDCR number. Returns inmate dict or None."""
    visible_inputs = await page.query_selector_all('input[type="text"]')
    target = None
    for inp in visible_inputs:
        if await inp.is_visible():
            target = inp
            break
    if not target:
        log.error("No visible text input found")
        return None

    await target.fill(cdcr_num)
    await page.wait_for_timeout(300)

    await page.click('button:has-text("SEARCH")')
    await page.wait_for_timeout(3000)

    text = await page.evaluate("() => document.body.innerText")

    # CDCR echoes the search term (e.g. "Search Result for CDCR Number - CC8109")
    # even when no inmate exists, so we must check explicit no-result markers
    # before treating the page as a hit.
    no_result_markers = [
        "No Results",
        "We cannot find a person",
        "0-0 of 0",
    ]
    if any(m in text for m in no_result_markers):
        return None

    if cdcr_num not in text:
        return None

    name = None
    facility = None
    admission_date = None

    lines = text.split('\n')
    for i, line in enumerate(lines):
        if cdcr_num in line and 'Search Result' not in line:
            if i > 0:
                candidate = lines[i - 1].strip()
                if candidate and candidate not in ('Name', '< RETURN TO SEARCH', 'CDCR', ''):
                    name = candidate

            for j in range(i + 1, min(i + 6, len(lines))):
                l = lines[j].strip()
                date_m = re.match(r'^([A-Z][a-z]{2}\s+\d{1,2}\s+\d{4})$', l)
                if date_m:
                    try:
                        admission_date = datetime.strptime(date_m.group(1), "%b %d %Y")
                    except ValueError:
                        pass
                if any(kw in l for kw in ['Prison', 'Institution', 'Facility', 'Correctional', 'Center']):
                    facility = l
            break

    # If we got past the no-result check but still can't find a name,
    # DO NOT store a phantom record. Return None and let the caller
    # log/skip. This fixes the bug where every non-result was being
    # saved as "UNKNOWN (CCxxxx)".
    if not name:
        log.warning("Page had no no-result marker but name parse failed; skipping",
                    cdcr=cdcr_num)
        return None

    return {
        "cdcr_num": cdcr_num,
        "name": name,
        "facility": facility,
        "admission_date": admission_date,
    }


async def save_progress(last_num: int, total: int, prefix: str = "CC",
                        status: str = "running"):
    async with async_session_factory() as session:
        result = await session.execute(
            select(ScrapeProgress).where(ScrapeProgress.state == "CA")
        )
        progress = result.scalar_one_or_none()
        if progress:
            progress.last_letter = prefix
            progress.last_page = last_num
            progress.total_found = total
            progress.status = status
            progress.last_updated = datetime.now(timezone.utc)
        else:
            progress = ScrapeProgress(
                state="CA", last_letter=prefix, last_page=last_num,
                total_found=total, status=status,
                started_at=datetime.now(timezone.utc),
            )
            session.add(progress)
        await session.commit()


async def load_inmate_to_db(rec: dict):
    async with async_session_factory() as session:
        existing = await session.execute(
            select(Inmate).where(
                Inmate.inmate_id == rec["cdcr_num"],
                Inmate.state == "CA",
            )
        )
        if existing.scalar_one_or_none():
            return False

        inmate = Inmate(
            inmate_id=rec["cdcr_num"],
            name=rec["name"],
            state="CA",
            facility=rec.get("facility"),
            status=InmateStatus.ACTIVE.value,
            source_url=CA_URL,
            discovered_at=datetime.now(timezone.utc),
            admission_date=rec.get("admission_date"),
        )
        session.add(inmate)
        await session.commit()
        return True


async def run(
    prefix: str = "CC",
    start_num: int = 7920,
    max_count: int = 50,
    stop_after_misses: int = 10,
):
    """
    Enumerate CDCR numbers and scrape inmate data.

    Args:
        prefix: CDCR# prefix (CC for men, WH for women)
        start_num: Starting sequence number
        max_count: Max numbers to check
        stop_after_misses: Stop after N consecutive not-found (higher than NY due to gaps)
    """
    await init_db()

    log.info("California CDCR scraper starting",
             prefix=prefix, start=start_num, max_count=max_count)

    found_total = 0
    consecutive_misses = 0
    current_num = start_num

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
            ignore_https_errors=True,
        )
        page = await context.new_page()
        if _USE_NEW_STEALTH:
            stealth = Stealth()
            await stealth.apply_stealth_async(page)
        else:
            await stealth_async(page)
        page.set_default_timeout(15000)

        await page.goto(CA_URL, wait_until="domcontentloaded", timeout=30000)
        await page.wait_for_timeout(3000)
        await dismiss_popup_and_agree(page)
        await select_cdcr_radio(page)

        for i in range(max_count):
            cdcr_num = f"{prefix}{current_num}"
            log.info("Searching CDCR#", cdcr=cdcr_num, progress=f"{i+1}/{max_count}")

            try:
                result = await search_cdcr(page, cdcr_num)

                if result:
                    log.info("Found inmate",
                             cdcr=cdcr_num, name=result["name"])
                    is_new = await load_inmate_to_db(result)
                    if is_new:
                        found_total += 1
                    consecutive_misses = 0
                else:
                    log.info("CDCR# not found (gap)", cdcr=cdcr_num)
                    consecutive_misses += 1

                    if consecutive_misses >= stop_after_misses:
                        log.info("Too many consecutive gaps — likely reached end",
                                 misses=consecutive_misses)
                        break

            except Exception as e:
                log.error("Error searching CDCR#", cdcr=cdcr_num, error=str(e))
                consecutive_misses += 1

            current_num += 1
            await save_progress(current_num, found_total, prefix)

            # Navigate back via "RETURN TO SEARCH" link or URL
            return_link = await page.query_selector('a:has-text("RETURN TO SEARCH")')
            if return_link:
                await return_link.click()
                await page.wait_for_timeout(1500)
            else:
                await page.goto(CA_URL, wait_until="domcontentloaded", timeout=30000)
                await page.wait_for_timeout(2000)
                await dismiss_popup_and_agree(page)
            await select_cdcr_radio(page)

            # Rate limit
            delay = settings.scraper_request_delay
            await page.wait_for_timeout(int(delay * 1000))

        await browser.close()

    await save_progress(current_num, found_total, prefix, status="completed")
    log.info("California scraper complete",
             found=found_total, last_num=current_num)
    return found_total


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Scrape CA CDCR by number enumeration")
    parser.add_argument("--prefix", default="CC", help="CDCR# prefix (default: CC)")
    parser.add_argument("--start", type=int, default=7920,
                        help="Start number (default: 7920)")
    parser.add_argument("--count", type=int, default=50,
                        help="Max numbers to check (default: 50)")
    parser.add_argument("--misses", type=int, default=10,
                        help="Stop after N consecutive misses (default: 10)")
    args = parser.parse_args()

    asyncio.run(run(
        prefix=args.prefix, start_num=args.start,
        max_count=args.count, stop_after_misses=args.misses,
    ))
