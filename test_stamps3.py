"""Inspect the Total Stamps tab and the per-contact package view."""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from config import settings
from securus.client import SecurusClient
from logger import get_logger

log = get_logger("test_stamps3")

EMESSAGE_STAMPS_URL = "https://securustech.online/#/products/emessage/stamps"


async def inspect():
    async with SecurusClient(headless=False) as client:
        await client.login()
        log.info("Login OK")

        # Go directly to the Total Stamps page via URL
        log.info("=== Navigating to Total Stamps page ===")
        await client.page.goto(
            EMESSAGE_STAMPS_URL + "/totalStamps",
            wait_until="domcontentloaded",
        )
        await client.page.wait_for_timeout(4000)
        await client._dismiss_overlays()
        await client._screenshot("total_stamps_page_direct")

        # Dump the page content looking for per-facility info
        text = await client.page.locator("body").text_content() or ""
        for line in text.split("\n"):
            stripped = line.strip()
            if stripped and len(stripped) > 3 and len(stripped) < 200:
                if any(kw in stripped.lower() for kw in [
                    "stamp", "available", "correction", "doc ", "facility",
                    "washington", "oklahoma", "new york", "california", "arkansas",
                    "universal",
                ]):
                    log.info("Total Stamps text", line=stripped)

        # Try to find a table or list with per-facility stamps
        tables = await client.page.evaluate("""
            () => {
                const tables = document.querySelectorAll('table');
                return Array.from(tables).map((t, i) => {
                    const rows = t.querySelectorAll('tr');
                    return {
                        index: i,
                        rows: Array.from(rows).map(r => r.textContent.trim()).slice(0, 20)
                    };
                });
            }
        """)
        for table in tables:
            log.info("Table found", index=table["index"],
                     rows=table["rows"][:10])

        # Also check for any divs/spans with stamp counts
        stamp_elements = await client.page.evaluate("""
            () => {
                const all = document.querySelectorAll('*');
                const results = [];
                for (const el of all) {
                    const text = el.textContent || '';
                    if (text.match(/\\d+\\s*stamp/i) && text.length < 200) {
                        results.push({
                            tag: el.tagName,
                            class: el.className,
                            text: text.trim().substring(0, 150)
                        });
                    }
                }
                return results.slice(0, 30);
            }
        """)
        for el in stamp_elements:
            log.info("Stamp element", tag=el["tag"], cls=el["class"],
                     text=el["text"][:100])

        # Now go to Purchase tab and select the first contact
        log.info("")
        log.info("=== Selecting a contact on Purchase tab ===")
        await client.page.goto(
            EMESSAGE_STAMPS_URL + "/purchase",
            wait_until="domcontentloaded",
        )
        await client.page.wait_for_timeout(3000)
        await client._dismiss_overlays()

        # The contact dropdown is the 3rd select (index 2)
        selects = client.page.locator("select:visible")
        sel_count = await selects.count()
        log.info("Visible selects", count=sel_count)

        contact_select = None
        for i in range(sel_count):
            sel = selects.nth(i)
            opt_count = await sel.evaluate("s => s.options.length")
            if opt_count > 50:
                contact_select = sel
                log.info("Found contact dropdown", index=i,
                         options=opt_count)
                break

        if not contact_select:
            log.error("Contact dropdown not found")
            return

        # Select the first real contact
        first_opt = await contact_select.evaluate("""
            s => {
                for (const o of s.options) {
                    if (o.value && o.text !== 'Select') return {value: o.value, text: o.text};
                }
                return null;
            }
        """)

        if first_opt:
            log.info("Selecting contact", name=first_opt["text"])
            await contact_select.select_option(value=first_opt["value"])
            await client.page.wait_for_timeout(3000)
            await client._screenshot("after_first_contact_selected")

            # Check what appeared — packages, facility name, stamp count
            page_text = await client.page.locator("body").text_content() or ""
            for phrase in ["Stamps Package", "stamps", "facility", "Stamps Available",
                           "Stamps Now", "Choose your"]:
                idx = page_text.lower().find(phrase.lower())
                if idx >= 0:
                    snippet = page_text[max(0, idx-30):idx+80].strip()
                    log.info("Found phrase", phrase=phrase,
                             context=snippet)

        log.info("Inspect complete — check screenshots")
        await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(inspect())
