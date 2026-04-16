"""
Probe CDCR search page to capture the actual HTML/text layout
for a known CDCR number, so we can fix the name parser correctly.

Usage:
  python probe_cdcr.py CC8109 CC8102 CC8087

Output: writes probe_cdcr_<num>.html and probe_cdcr_<num>.txt for each number,
plus prints what name/facility our current parser finds vs what's actually there.
"""

import asyncio
import re
import sys
from datetime import datetime
from pathlib import Path

from playwright.async_api import async_playwright

try:
    from playwright_stealth import Stealth
    _USE_NEW_STEALTH = True
except ImportError:
    from playwright_stealth import stealth_async
    _USE_NEW_STEALTH = False


CA_URL = "https://ciris.mt.cdcr.ca.gov/search"
OUT_DIR = Path("./probe_output")


async def dismiss_popup_and_agree(page):
    close_btn = await page.query_selector('button:has-text("Close")')
    if close_btn and await close_btn.is_visible():
        await close_btn.click()
        await page.wait_for_timeout(1000)
    agree_btn = await page.query_selector('button:has-text("agree")')
    if agree_btn and await agree_btn.is_visible():
        await agree_btn.click()
        await page.wait_for_timeout(2000)


async def select_cdcr_radio(page):
    label = await page.query_selector('label:has-text("CDCR Number")')
    if label:
        await label.click()
        await page.wait_for_timeout(500)


async def probe_one(page, cdcr_num: str):
    print(f"\n{'=' * 70}")
    print(f"PROBING: {cdcr_num}")
    print('=' * 70)

    visible_inputs = await page.query_selector_all('input[type="text"]')
    target = None
    for inp in visible_inputs:
        if await inp.is_visible():
            target = inp
            break
    if not target:
        print("  ERROR: no visible text input")
        return

    await target.fill(cdcr_num)
    await page.wait_for_timeout(300)
    await page.click('button:has-text("SEARCH")')
    await page.wait_for_timeout(3500)

    html = await page.content()
    text = await page.evaluate("() => document.body.innerText")

    OUT_DIR.mkdir(exist_ok=True)
    (OUT_DIR / f"probe_{cdcr_num}.html").write_text(html)
    (OUT_DIR / f"probe_{cdcr_num}.txt").write_text(text)

    print(f"  Saved: probe_output/probe_{cdcr_num}.html and .txt")
    print(f"  Page text length: {len(text)} chars")
    print(f"  Contains CDCR#: {cdcr_num in text}")
    print(f"  Contains '0-0 of 0': {'0-0 of 0' in text}")

    print("\n  --- TEXT LINES (first 80) ---")
    lines = text.split("\n")
    for i, line in enumerate(lines[:80]):
        stripped = line.strip()
        marker = " <-- CDCR#" if cdcr_num in line else ""
        if stripped or marker:
            print(f"    {i:3d}: {stripped[:100]}{marker}")

    print("\n  --- PARSER RESULT (current broken logic) ---")
    name = None
    facility = None
    for i, line in enumerate(lines):
        if cdcr_num in line and "Search Result" not in line:
            if i > 0:
                candidate = lines[i - 1].strip()
                if candidate and candidate not in ("Name", "< RETURN TO SEARCH", "CDCR", ""):
                    name = candidate
            for j in range(i + 1, min(i + 6, len(lines))):
                lj = lines[j].strip()
                if any(kw in lj for kw in ["Prison", "Institution", "Facility",
                                           "Correctional", "Center"]):
                    facility = lj
            break
    print(f"    name={name!r}")
    print(f"    facility={facility!r}")

    try:
        table_html = await page.evaluate("""() => {
            const tables = document.querySelectorAll('table');
            return Array.from(tables).map(t => t.outerHTML).slice(0, 2);
        }""")
        for ti, thtml in enumerate(table_html):
            preview = re.sub(r'\s+', ' ', thtml)[:800]
            print(f"\n  --- TABLE[{ti}] (first 800 chars) ---")
            print(f"    {preview}")
    except Exception as e:
        print(f"  table probe failed: {e}")

    return_link = await page.query_selector('a:has-text("RETURN TO SEARCH")')
    if return_link:
        await return_link.click()
        await page.wait_for_timeout(1500)
    else:
        await page.goto(CA_URL, wait_until="domcontentloaded", timeout=30000)
        await page.wait_for_timeout(2000)
        await dismiss_popup_and_agree(page)
    await select_cdcr_radio(page)


async def main(numbers: list[str]):
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=False,
            args=["--no-sandbox", "--disable-blink-features=AutomationControlled"],
        )
        context = await browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent=("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/131.0.0.0 Safari/537.36"),
            ignore_https_errors=True,
        )
        page = await context.new_page()
        if _USE_NEW_STEALTH:
            await Stealth().apply_stealth_async(page)
        else:
            await stealth_async(page)
        page.set_default_timeout(15000)

        await page.goto(CA_URL, wait_until="domcontentloaded", timeout=30000)
        await page.wait_for_timeout(3000)
        await dismiss_popup_and_agree(page)
        await select_cdcr_radio(page)

        for num in numbers:
            try:
                await probe_one(page, num)
            except Exception as e:
                print(f"\n  FAILED probing {num}: {e}")

        print(f"\n\nAll probes saved to {OUT_DIR.resolve()}")
        print("Browser will stay open for 15s so you can inspect visually...")
        await asyncio.sleep(15)
        await browser.close()


if __name__ == "__main__":
    nums = sys.argv[1:] or ["CC8109", "CC8102", "CC8087"]
    asyncio.run(main(nums))
