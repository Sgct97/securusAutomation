"""
Stamp buying test — walks through the full flow WITHOUT clicking Buy.

Tests:
  1. Login
  2. Navigate to Purchase Stamps page
  3. Read stamp balances from dropdown
  4. Select a contact for one state
  5. Select a package
  6. Click Next (the flaky step)
  7. Screenshot the confirmation page
  8. STOP — does NOT click Buy

Usage:
  cd /opt/securusAutomation
  source venv/bin/activate
  python test_stamps.py
"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from config import settings
from securus.client import (
    SecurusClient, STAMP_PACKAGES, AGENCY_TO_STATE, StampPurchaseResult,
)
from logger import get_logger

log = get_logger("test_stamps")

STATE_TO_AGENCY = {
    "WA": "Washington State Department of Corrections",
    "OK": "Oklahoma Department of Corrections",
    "NY": "NYS DOCCS Inmate Services",
    "CA": "California Department of Corrections & Rehabilitation",
    "AR": "Arkansas DOC",
}


async def test_stamp_flow():
    async with SecurusClient(headless=settings.headless) as client:
        # Step 1: Login
        log.info("Step 1: Logging in")
        await client.login()
        log.info("Login successful")

        # Step 2: Read balances
        log.info("Step 2: Reading stamp balances")
        balances = await client.get_stamp_balances()
        log.info("Stamp balances", balances=balances)

        if not balances:
            log.warning("No balances found — dropdown may have unexpected format")
            log.info("Check screenshots in data/securus_debug/")
            return

        # Step 3: Pick a state to test with (use whichever has the most stamps)
        test_state = max(balances, key=balances.get) if balances else "WA"
        test_agency = STATE_TO_AGENCY.get(test_state, "")
        test_package = 6  # smallest package
        log.info("Step 3: Testing purchase flow (NO actual buy)",
                 state=test_state, agency=test_agency, package=test_package)

        # Step 4: Navigate to Purchase Stamps
        log.info("Step 4: Navigating to Purchase Stamps page")
        if not await client._navigate_to_purchase_stamps():
            log.error("Failed to navigate to Purchase Stamps")
            return

        # Step 5: Select contact from dropdown
        log.info("Step 5: Selecting contact from dropdown")
        contact_dropdown = client.page.locator("select").first
        await contact_dropdown.wait_for(state="visible", timeout=10000)

        dropdown_info = await contact_dropdown.evaluate("""
            (sel) => {
                const result = [];
                const groups = sel.querySelectorAll('optgroup');
                if (groups.length > 0) {
                    groups.forEach(g => {
                        g.querySelectorAll('option').forEach(o => {
                            result.push({
                                value: o.value, text: o.text,
                                group: g.label || ''
                            });
                        });
                    });
                } else {
                    sel.querySelectorAll('option').forEach(o => {
                        result.push({
                            value: o.value, text: o.text, group: ''
                        });
                    });
                }
                return result;
            }
        """)

        log.info("Dropdown contents",
                 total_options=len(dropdown_info),
                 sample=[f"{o['group']} / {o['text']}" for o in dropdown_info[:10]])

        target_option = None
        agency_lower = test_agency.lower()
        for opt in dropdown_info:
            if not opt["value"]:
                continue
            group_lower = opt.get("group", "").lower()
            text_lower = opt.get("text", "").lower()
            if agency_lower in group_lower or agency_lower in text_lower:
                target_option = opt
                break

        if not target_option:
            log.error("No contact found for state", state=test_state,
                      agency=test_agency)
            await client._screenshot("test_no_contact_found")
            return

        await contact_dropdown.select_option(value=target_option["value"])
        log.info("Contact selected", contact=target_option["text"])
        await client.page.wait_for_timeout(2000)
        await client._screenshot("test_contact_selected")

        # Step 6: Select package
        log.info("Step 6: Selecting package")
        pkg_label = f"{test_package} Stamps Package"
        from playwright.async_api import TimeoutError as PwTimeout

        pkg_locator = client.page.locator(f"text='{pkg_label}'").first
        try:
            await pkg_locator.wait_for(state="visible", timeout=5000)
            await pkg_locator.click()
            log.info("Package selected", package=pkg_label)
        except PwTimeout:
            radio = client.page.locator(
                f"input[type='radio']:near(:text('{pkg_label}'))"
            ).first
            try:
                await radio.click(timeout=5000)
                log.info("Package radio clicked", package=pkg_label)
            except PwTimeout:
                log.error("Could not find/select package", package=pkg_label)
                await client._screenshot("test_package_not_found")
                return

        await client._screenshot("test_package_selected")

        # Step 7: Click Next (the flaky step)
        log.info("Step 7: Clicking Next")
        next_btn = client.page.locator(
            "button:has-text('Next'), a:has-text('Next'), "
            "button:has-text('NEXT'), a:has-text('NEXT')"
        ).first
        await next_btn.wait_for(state="visible", timeout=10000)
        await next_btn.click()
        log.info("Clicked Next")
        await client.page.wait_for_timeout(4000)

        if "/login" in client.page.url:
            log.warning("GOT LOGGED OUT after clicking Next (known bug)")
            await client._screenshot("test_logged_out_after_next")
            log.info("This confirms the logout bug exists. Retry logic will handle it.")
            return

        await client._screenshot("test_confirmation_page")
        log.info("Step 7 SUCCESS: Confirmation page reached!")
        log.info("STOPPING HERE — did NOT click Buy")

        page_text = await client.page.locator("body").text_content() or ""
        log.info("Confirmation page text (first 500 chars)",
                 text=page_text[:500])

        log.info("=" * 60)
        log.info("TEST COMPLETE — all steps passed, no money spent")
        log.info("=" * 60)


if __name__ == "__main__":
    asyncio.run(test_stamp_flow())
