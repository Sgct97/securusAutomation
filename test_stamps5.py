"""Test stamp flow end-to-end with retry on logout at EVERY step.
Reads balances, selects contact, picks package, clicks Next —
STOPS before Buy."""

import asyncio
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from playwright.async_api import TimeoutError as PwTimeout
from securus.client import SecurusClient
from logger import get_logger

log = get_logger("test_stamps5")

MAX_ATTEMPTS = 15


async def test():
    async with SecurusClient(headless=False) as client:
        await client.login()
        log.info("Login OK")

        # ── Step 1: Read balances (has its own retry) ──
        log.info("=== Step 1: get_stamp_balances ===")
        balances = await client.get_stamp_balances()
        log.info("Balances", balances=balances)

        # ── Step 2: Purchase flow with retry on logout at any step ──
        for attempt in range(1, MAX_ATTEMPTS + 1):
            log.info("=== Step 2: Purchase attempt ===", attempt=attempt)

            try:
                await client._ensure_logged_in()
                await client._goto_or_relogin(client.STAMPS_PURCHASE_URL)

                # Find contact dropdown
                selects = client.page.locator("select:visible")
                contact_dropdown = None
                for i in range(await selects.count()):
                    sel = selects.nth(i)
                    cnt = await sel.evaluate("s => s.options.length")
                    if cnt > 50:
                        contact_dropdown = sel
                        break

                if not contact_dropdown:
                    log.warning("No contact dropdown, retrying", attempt=attempt)
                    await asyncio.sleep(5)
                    continue

                await contact_dropdown.select_option(label="JUAN SOTELO")
                log.info("Selected JUAN SOTELO")
                await client.page.wait_for_timeout(3000)

                # Check for logout after contact selection
                if "/login" in client.page.url:
                    log.warning("Logged out after selecting contact, retrying",
                                attempt=attempt)
                    client._logged_in = False
                    await asyncio.sleep(5)
                    continue

                await client._screenshot("test5_after_contact")

                text = await client.page.locator("body").text_content() or ""
                m = re.search(r"(\d+)\s*Stamps?\s*Available", text)
                if m:
                    log.info("Stamps available", count=m.group(1))

                # Select 6 Stamps Package
                await client.page.locator("text=6 Stamps Package").first.click(
                    timeout=5000)
                log.info("Selected 6 Stamps Package")

                # Check for logout after package selection
                if "/login" in client.page.url:
                    log.warning("Logged out after selecting package, retrying",
                                attempt=attempt)
                    client._logged_in = False
                    await asyncio.sleep(5)
                    continue

                # Click Next
                next_btn = client.page.locator(
                    "button:has-text('Next'), button:has-text('NEXT')"
                ).first
                await next_btn.click()
                log.info("Clicked Next")
                await client.page.wait_for_timeout(4000)

                if "/login" in client.page.url:
                    log.warning("Logged out after Next, retrying",
                                attempt=attempt)
                    client._logged_in = False
                    await asyncio.sleep(5)
                    continue

                await client._screenshot("test5_confirmation_page")
                log.info("Confirmation page reached — NOT clicking Buy")
                break  # success

            except PwTimeout as e:
                log.warning("Timeout during purchase flow, retrying",
                            error=str(e)[:100], attempt=attempt)
                client._logged_in = False
                await asyncio.sleep(5)
                continue
            except Exception as e:
                log.error("Unexpected error", error=str(e)[:200],
                          attempt=attempt)
                client._logged_in = False
                await asyncio.sleep(5)
                continue
        else:
            log.error("Failed all attempts")

        log.info("=== TEST COMPLETE ===")
        await asyncio.sleep(3)

if __name__ == "__main__":
    asyncio.run(test())
