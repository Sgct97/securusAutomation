"""Quick test to inspect the Purchase Stamps page structure."""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from config import settings
from securus.client import SecurusClient
from logger import get_logger

log = get_logger("test_stamps2")


async def inspect():
    async with SecurusClient(headless=False) as client:
        await client.login()
        log.info("Login OK")

        # Navigate to Purchase Stamps
        await client._navigate_to_purchase_stamps()

        # Dump ALL selects on the page
        all_selects = await client.page.evaluate("""
            () => {
                const selects = document.querySelectorAll('select');
                return Array.from(selects).map((sel, i) => {
                    const groups = sel.querySelectorAll('optgroup');
                    const groupData = Array.from(groups).map(g => ({
                        label: g.label,
                        options: Array.from(g.options).map(o => ({
                            value: o.value, text: o.text
                        }))
                    }));
                    const optData = Array.from(sel.options).map(o => ({
                        value: o.value, text: o.text
                    }));
                    return {
                        index: i,
                        id: sel.id,
                        name: sel.name,
                        className: sel.className,
                        optgroups: groupData,
                        options: optData
                    };
                });
            }
        """)

        for sel in all_selects:
            log.info("SELECT found", index=sel["index"], id=sel["id"],
                     name=sel["name"], num_options=len(sel["options"]),
                     num_optgroups=len(sel["optgroups"]))
            for opt in sel["options"][:15]:
                log.info("  option", value=opt["value"], text=opt["text"])
            for grp in sel["optgroups"][:5]:
                log.info("  optgroup", label=grp["label"],
                         opts=[o["text"] for o in grp["options"][:5]])

        # Now click on "Total Stamps" tab to see per-state breakdown
        log.info("Clicking Total Stamps tab")
        try:
            total_tab = client.page.locator("text=Total Stamps").first
            await total_tab.click()
            await client.page.wait_for_timeout(3000)
            await client._screenshot("total_stamps_tab")
            log.info("Total Stamps tab screenshot taken")

            page_text = await client.page.locator("body").text_content() or ""
            for line in page_text.split("\n"):
                line = line.strip()
                if line and ("stamp" in line.lower() or "available" in line.lower()
                             or "correction" in line.lower() or "doc" in line.lower()):
                    log.info("Relevant text", line=line)
        except Exception as e:
            log.error("Could not click Total Stamps", error=str(e))

        # Also select the first real contact to see what packages appear
        log.info("Going back to Purchase tab to try selecting a contact")
        try:
            purchase_tab = client.page.locator("text=Purchase").first
            await purchase_tab.click()
            await client.page.wait_for_timeout(2000)
        except Exception:
            await client._navigate_to_purchase_stamps()

        contact_select = client.page.locator("select").first
        await contact_select.wait_for(state="visible", timeout=5000)
        options = await contact_select.evaluate(
            "sel => Array.from(sel.options).map(o => ({value: o.value, text: o.text}))"
        )
        real_opts = [o for o in options if o["value"] and o["text"] != "Select"]
        if real_opts:
            log.info("Selecting first contact", contact=real_opts[0]["text"])
            await contact_select.select_option(value=real_opts[0]["value"])
            await client.page.wait_for_timeout(3000)
            await client._screenshot("after_contact_selected")
            log.info("Screenshot after contact selection taken")
        else:
            log.warning("No contacts found in dropdown")

        log.info("Inspect complete — check data/securus_debug/ for screenshots")
        await asyncio.sleep(10)


if __name__ == "__main__":
    asyncio.run(inspect())
