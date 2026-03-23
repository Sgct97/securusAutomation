"""
WA DOC Detail Page Recon

Clicks into inmate detail pages from different spots in the paginated list
to discover if there's any ordering (by admission date, DOC#, alpha, etc.)
and what fields are available on the detail page.

Read-only — just screenshots and logs.
"""

import asyncio
import re
from pathlib import Path

from playwright.async_api import async_playwright, Page

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from playwright_stealth import Stealth
from logger import get_logger

log = get_logger("scraper.wa_recon")

WA_URL = "https://doc.wa.gov/information/inmate-search/"
SCREENSHOT_DIR = Path(__file__).resolve().parent.parent / "data" / "wa_detail_recon"


async def click_to_page(page: Page, target: int) -> bool:
    """Navigate to a specific page number."""
    for _ in range(1, target):
        btn = await page.query_selector("a:has-text('Next')")
        if not btn:
            return False
        cls = await btn.get_attribute("class") or ""
        if "disabled" in cls:
            return False
        await btn.click()
        await page.wait_for_load_state("networkidle", timeout=15000)
        await page.wait_for_timeout(500)
    return True


async def get_list_rows(page: Page) -> list[dict]:
    """Extract rows from the current list page."""
    rows = await page.query_selector_all("table tbody tr")
    results = []
    for row in rows:
        cells = await row.query_selector_all("td")
        if len(cells) < 4:
            continue
        doc_num = (await cells[0].text_content() or "").strip()
        name = (await cells[1].text_content() or "").strip().replace("(link is external)", "").strip()
        age = (await cells[2].text_content() or "").strip()
        location = (await cells[3].text_content() or "").strip().replace("(link is external)", "").strip()
        if doc_num and doc_num.isdigit():
            results.append({"doc": doc_num, "name": name, "age": age, "loc": location})
    return results


async def scrape_detail(page: Page, doc_num: str, index: int) -> dict:
    """Click into an inmate's detail page and extract all available info."""
    # Click the DOC number link
    link = await page.query_selector(f"a:has-text('{doc_num}')")
    if not link:
        # Try clicking the name cell instead
        link = await page.query_selector(f"td a")
    
    if not link:
        log.warning("No clickable link for DOC", doc=doc_num)
        return {"doc": doc_num, "error": "no link"}

    await link.click()
    await page.wait_for_load_state("networkidle", timeout=15000)
    await page.wait_for_timeout(1500)

    SCREENSHOT_DIR.mkdir(parents=True, exist_ok=True)
    await page.screenshot(
        path=str(SCREENSHOT_DIR / f"detail_{index:02d}_{doc_num}.png"),
        full_page=True,
    )

    content = await page.content()
    body_text = await page.evaluate("() => document.body.innerText")

    info = {"doc": doc_num, "raw_text": body_text[:2000]}

    # Try to extract common fields
    for field in ["Admission Date", "Date Received", "Reception Date",
                  "Earliest Release Date", "Current Facility",
                  "Date of Birth", "Race", "Sex", "Sentence",
                  "Admission", "Confinement", "Begin Date"]:
        match = re.search(
            rf'{field}[:\s]*([^\n<]+)',
            body_text, re.IGNORECASE,
        )
        if match:
            info[field.lower().replace(" ", "_")] = match.group(1).strip()

    log.info("Detail page", doc=doc_num, fields=list(info.keys()))
    return info


async def main():
    SCREENSHOT_DIR.mkdir(parents=True, exist_ok=True)

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=False,
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
        stealth = Stealth()
        page = await context.new_page()
        await stealth.apply_stealth_async(page)
        page.set_default_timeout(15000)

        # Check page 1 (first 3 inmates)
        log.info("=" * 50)
        log.info("PAGE 1 — first inmates in the list")
        log.info("=" * 50)
        await page.goto(WA_URL)
        await page.wait_for_load_state("networkidle", timeout=20000)
        await page.wait_for_timeout(1000)

        rows_p1 = await get_list_rows(page)
        log.info("Page 1 rows (first 5):")
        for r in rows_p1[:5]:
            log.info("  Row", doc=r["doc"], name=r["name"], age=r["age"], loc=r["loc"])

        # Click into first inmate's detail
        if rows_p1:
            detail1 = await scrape_detail(page, rows_p1[0]["doc"], 1)
            log.info("Detail page 1 text:", text=detail1.get("raw_text", "")[:500])

        # Go back and check page 1, 3rd inmate
        await page.goto(WA_URL)
        await page.wait_for_load_state("networkidle", timeout=15000)
        await page.wait_for_timeout(1000)
        if len(rows_p1) >= 3:
            detail2 = await scrape_detail(page, rows_p1[2]["doc"], 2)
            log.info("Detail page 3 text:", text=detail2.get("raw_text", "")[:500])

        # Check a middle page
        log.info("=" * 50)
        log.info("PAGE 350 — middle of the list")
        log.info("=" * 50)
        await page.goto(WA_URL)
        await page.wait_for_load_state("networkidle", timeout=15000)
        await page.wait_for_timeout(1000)
        await click_to_page(page, 350)
        rows_mid = await get_list_rows(page)
        log.info("Page 350 rows (first 5):")
        for r in rows_mid[:5]:
            log.info("  Row", doc=r["doc"], name=r["name"], age=r["age"], loc=r["loc"])

        if rows_mid:
            detail3 = await scrape_detail(page, rows_mid[0]["doc"], 3)
            log.info("Detail mid text:", text=detail3.get("raw_text", "")[:500])

        # Check last page area
        log.info("=" * 50)
        log.info("PAGE 695 — near the end")
        log.info("=" * 50)
        await page.goto(WA_URL)
        await page.wait_for_load_state("networkidle", timeout=15000)
        await page.wait_for_timeout(1000)
        await click_to_page(page, 695)
        rows_end = await get_list_rows(page)
        log.info("Page 695 rows (first 5):")
        for r in rows_end[:5]:
            log.info("  Row", doc=r["doc"], name=r["name"], age=r["age"], loc=r["loc"])

        if rows_end:
            detail4 = await scrape_detail(page, rows_end[0]["doc"], 4)
            log.info("Detail end text:", text=detail4.get("raw_text", "")[:500])

        log.info("=" * 50)
        log.info("RECON COMPLETE")
        log.info("=" * 50)

        await page.wait_for_timeout(10000)
        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
