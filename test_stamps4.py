"""Focused test: navigate Total Stamps tab and select one contact."""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from config import settings
from securus.client import SecurusClient
from logger import get_logger

log = get_logger("test_stamps4")


async def inspect():
    async with SecurusClient(headless=False) as client:
        await client.login()
        log.info("Login OK")

        # Try direct URL to Total Stamps page
        log.info("=== Navigating to Total Stamps via direct URL ===")
        await client.page.goto(
            "https://securustech.online/#/products/emessage/stamps/totalStamps",
            wait_until="domcontentloaded",
        )
        await client.page.wait_for_timeout(5000)
        await client._dismiss_overlays()
        await client._screenshot("total_stamps_direct_url")
        log.info("Screenshot taken: total_stamps_direct_url")

        # Also try clicking the actual tab link
        log.info("=== Trying to click Total Stamps tab ===")
        tab = client.page.locator("a:has-text('Total Stamps'), span:has-text('Total Stamps')").first
        try:
            await tab.click()
            await client.page.wait_for_timeout(4000)
            await client._screenshot("total_stamps_after_click")
            log.info("Screenshot taken: total_stamps_after_click")
        except Exception as e:
            log.warning("Tab click failed", error=str(e))

        # Dump all visible text on this page
        log.info("=== Page structure ===")
        # Look for tables
        table_data = await client.page.evaluate("""
            () => {
                const results = [];
                // Check all table rows
                document.querySelectorAll('tr').forEach(tr => {
                    results.push({type: 'tr', text: tr.textContent.trim().substring(0, 200)});
                });
                // Check all list items
                document.querySelectorAll('li').forEach(li => {
                    const t = li.textContent.trim();
                    if (t.length > 2 && t.length < 200)
                        results.push({type: 'li', text: t});
                });
                // Check divs with "stamp" text
                document.querySelectorAll('div, span, p, td, th').forEach(el => {
                    const t = (el.textContent || '').trim();
                    if (t.length > 3 && t.length < 150 &&
                        (t.toLowerCase().includes('stamp') ||
                         t.toLowerCase().includes('facility') ||
                         t.toLowerCase().includes('correction') ||
                         t.toLowerCase().includes('universal'))) {
                        results.push({
                            type: el.tagName,
                            class: el.className.substring(0, 50),
                            text: t
                        });
                    }
                });
                return results.slice(0, 40);
            }
        """)
        for item in table_data:
            log.info("Element", **item)

        # Now go to Purchase and select a specific known contact
        log.info("")
        log.info("=== Selecting JUAN SOTELO on Purchase tab ===")
        await client.page.goto(
            "https://securustech.online/#/products/emessage/stamps/purchase",
            wait_until="domcontentloaded",
        )
        await client.page.wait_for_timeout(3000)
        await client._dismiss_overlays()

        # Find the contact dropdown (the one with 400+ options)
        contact_select = None
        selects = client.page.locator("select:visible")
        for i in range(await selects.count()):
            sel = selects.nth(i)
            count = await sel.evaluate("s => s.options.length")
            if count > 50:
                contact_select = sel
                break

        if contact_select:
            await contact_select.select_option(label="JUAN SOTELO")
            log.info("Selected JUAN SOTELO")
            await client.page.wait_for_timeout(3000)
            await client._screenshot("after_juan_sotelo_selected")

            # Read what appeared — facility, stamp count, packages
            page_text = await client.page.locator("body").text_content() or ""
            for kw in ["Stamps Package", "Available", "facility",
                       "Washington", "Oklahoma", "California", "New York",
                       "Arkansas", "Correction", "DOC"]:
                idx = page_text.lower().find(kw.lower())
                if idx >= 0:
                    snippet = page_text[max(0, idx-50):idx+100].strip()
                    log.info("Found", keyword=kw, context=snippet)

        log.info("Done — check data/securus_debug/ for screenshots")
        await asyncio.sleep(10)


if __name__ == "__main__":
    asyncio.run(inspect())
