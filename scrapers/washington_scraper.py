"""
Washington DOC Production Scraper

Scrapes the paginated inmate list at https://doc.wa.gov/information/inmate-search/
Uses differential tracking to identify NEW inmates:
  - First run: builds a baseline of all known DOC numbers (no outreach)
  - Subsequent runs: any DOC number not previously seen is flagged as new

Table columns: DOC Number, Name, Age, Location (no reception date on list page).
"""

import asyncio
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
from sqlalchemy import select, func

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config import settings
from database import (
    Inmate, InmateStatus, ScrapeProgress,
    init_db, async_session_factory,
)
from logger import get_logger

log = get_logger("scraper.washington")

WA_BASE_URL = "https://doc.wa.gov/records/incarcerated-data-search/incarcerated-search"
VINELINK_DETAIL_URL = "https://vinelink.vineapps.com/offender-detail/48626/900/{doc}"


async def scrape_page(page: Page, page_num: int) -> list[dict]:
    """Navigate to a specific page via URL param and extract inmate rows."""
    url = WA_BASE_URL if page_num == 1 else f"{WA_BASE_URL}?page={page_num - 1}"
    await page.goto(url, wait_until="networkidle", timeout=20000)
    await page.wait_for_timeout(1000)

    rows = await page.query_selector_all("table tbody tr")
    inmates = []

    for row in rows:
        cells = await row.query_selector_all("td")
        if len(cells) < 4:
            continue

        doc_num = (await cells[0].text_content() or "").strip()
        raw_name = (await cells[1].text_content() or "").strip()
        name = raw_name.replace("(link is external)", "").strip()
        age = (await cells[2].text_content() or "").strip()
        location = (await cells[3].text_content() or "").strip().replace("(link is external)", "").strip()

        if doc_num and doc_num.isdigit():
            inmates.append({
                "doc_number": doc_num,
                "name": name,
                "age": age,
                "location": location,
                "vinelink_url": VINELINK_DETAIL_URL.format(doc=doc_num),
            })

    return inmates


async def save_progress(last_page: int, total_found: int, status: str = "running"):
    """Update scrape progress in the database."""
    async with async_session_factory() as session:
        result = await session.execute(
            select(ScrapeProgress).where(ScrapeProgress.state == "WA")
        )
        progress = result.scalar_one_or_none()
        if progress:
            progress.last_page = last_page
            progress.total_found = total_found
            progress.status = status
            progress.last_updated = datetime.now(timezone.utc)
        else:
            progress = ScrapeProgress(
                state="WA", last_page=last_page,
                total_found=total_found, status=status,
                started_at=datetime.now(timezone.utc),
            )
            session.add(progress)
        await session.commit()


async def get_known_doc_numbers() -> set[str]:
    """Get the set of all WA DOC numbers already in the database."""
    async with async_session_factory() as session:
        result = await session.execute(
            select(Inmate.inmate_id).where(Inmate.state == "WA")
        )
        return {row[0] for row in result.all()}


async def load_inmates_to_db(
    inmates: list[dict],
    known_ids: set[str],
    batch_size: int = 200,
) -> tuple[int, int, list[dict]]:
    """
    Load scraped inmates into the database.
    Tracks which ones are genuinely new (not previously known).

    Returns:
        (inserted_count, updated_count, list_of_new_inmates)
    """
    inserted = 0
    updated = 0
    new_inmates = []

    async with async_session_factory() as session:
        for rec in inmates:
            is_previously_known = rec["doc_number"] in known_ids

            existing = await session.execute(
                select(Inmate).where(
                    Inmate.inmate_id == rec["doc_number"],
                    Inmate.state == "WA",
                )
            )
            existing_inmate = existing.scalar_one_or_none()

            if existing_inmate:
                existing_inmate.name = rec["name"]
                existing_inmate.facility = rec["location"]
                existing_inmate.last_verified = datetime.now(timezone.utc)
                existing_inmate.status = InmateStatus.ACTIVE.value
                updated += 1
            else:
                inmate = Inmate(
                    inmate_id=rec["doc_number"],
                    name=rec["name"],
                    state="WA",
                    facility=rec["location"],
                    status=InmateStatus.ACTIVE.value,
                    source_url=WA_BASE_URL,
                )
                session.add(inmate)
                inserted += 1

                if is_previously_known:
                    # Shouldn't happen, but just in case
                    pass
                else:
                    new_inmates.append(rec)

            if (inserted + updated) % batch_size == 0:
                await session.commit()

        await session.commit()

    return inserted, updated, new_inmates


async def mark_released(current_ids: set[str]):
    """
    Mark inmates no longer on the active list as released.
    Only marks those previously ACTIVE who are now missing.
    """
    marked = 0
    async with async_session_factory() as session:
        active_wa = await session.execute(
            select(Inmate).where(
                Inmate.state == "WA",
                Inmate.status == InmateStatus.ACTIVE.value,
            )
        )
        for inmate in active_wa.scalars().all():
            if inmate.inmate_id not in current_ids:
                inmate.status = InmateStatus.RELEASED.value
                marked += 1

        if marked:
            await session.commit()

    return marked


async def run(max_pages: Optional[int] = None, start_page: int = 1):
    """
    Main scraper entry point.
    Uses URL-based pagination (?page=N) for reliability.
    Saves to DB after every page to prevent data loss.
    """
    await init_db()

    known_ids = await get_known_doc_numbers()
    is_baseline_run = len(known_ids) == 0

    log.info("Washington DOC scraper starting",
             max_pages=max_pages, start_page=start_page,
             known_inmates=len(known_ids),
             mode="BASELINE" if is_baseline_run else "DIFFERENTIAL")

    total_scraped = 0
    total_inserted = 0
    total_updated = 0
    all_new_inmates = []
    all_current_ids = set()

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

        end_page = start_page + (max_pages or 9999) - 1
        consecutive_empty = 0

        for page_num in range(start_page, end_page + 1):
            log.info("Scraping page", page=page_num)

            try:
                inmates = await scrape_page(page, page_num)
            except Exception as e:
                log.error("Page scrape failed — retrying once", page=page_num, error=str(e))
                await page.wait_for_timeout(5000)
                try:
                    inmates = await scrape_page(page, page_num)
                except Exception as e2:
                    log.error("Page scrape failed twice — skipping", page=page_num, error=str(e2))
                    consecutive_empty += 1
                    if consecutive_empty >= 3:
                        log.warning("3 consecutive failures — stopping")
                        break
                    continue

            if not inmates:
                consecutive_empty += 1
                if consecutive_empty >= 3:
                    log.warning("3 consecutive empty pages — stopping")
                    break
            else:
                consecutive_empty = 0
                total_scraped += len(inmates)

                for rec in inmates:
                    all_current_ids.add(rec["doc_number"])

                inserted, updated, new_inmates = await load_inmates_to_db(
                    inmates, known_ids
                )
                total_inserted += inserted
                total_updated += updated
                all_new_inmates.extend(new_inmates)

            log.info("Page saved", page=page_num,
                     found=len(inmates), total=total_scraped,
                     inserted=total_inserted, updated=total_updated)

            await save_progress(page_num, total_scraped)

            delay = settings.scraper_request_delay
            await page.wait_for_timeout(int(delay * 1000))

        await browser.close()

    released = 0
    if not max_pages and all_current_ids:
        released = await mark_released(all_current_ids)

    await save_progress(page_num, total_scraped, status="completed")

    log.info("Scrape complete",
             mode="BASELINE" if is_baseline_run else "DIFFERENTIAL",
             total_scraped=total_scraped,
             inserted=total_inserted,
             updated=total_updated,
             new=len(all_new_inmates),
             released=released)

    if all_new_inmates:
        for ni in all_new_inmates[:20]:
            log.info("NEW inmate", doc=ni["doc_number"],
                     name=ni["name"], facility=ni["location"])
        if len(all_new_inmates) > 20:
            log.info(f"... and {len(all_new_inmates) - 20} more")

    return total_scraped, all_new_inmates


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Scrape WA DOC inmate list")
    parser.add_argument("--max-pages", type=int, default=None,
                        help="Max pages to scrape (default: all)")
    parser.add_argument("--start-page", type=int, default=1,
                        help="Page to start from (default: 1)")
    args = parser.parse_args()

    total, new_inmates = asyncio.run(
        run(max_pages=args.max_pages, start_page=args.start_page)
    )
    print(f"\nTotal scraped: {total}")
    print(f"New inmates: {len(new_inmates)}")
