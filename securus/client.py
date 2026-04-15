"""
Securus eMessaging Platform — Production Client

Handles:
- Login with session management and re-auth on expiry
- Adding inmates as contacts via Securus Debit "Find Contact"
- Sending eMessages via the Compose flow
- Rate limiting, retries, screenshots on error, structured logging
"""

import asyncio
import random
import re
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

from playwright.async_api import (
    async_playwright,
    Browser,
    BrowserContext,
    Page,
    Playwright,
    TimeoutError as PwTimeout,
)
try:
    from playwright_stealth import Stealth
    _USE_NEW_STEALTH = True
except ImportError:
    from playwright_stealth import stealth_async
    _USE_NEW_STEALTH = False
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config import settings
from logger import get_logger

log = get_logger("securus.client")

SCREENSHOT_DIR = Path(__file__).resolve().parent.parent / "data" / "securus_debug"


@dataclass
class ContactResult:
    """Result of attempting to add an inmate as a contact."""
    success: bool
    inmate_id: str
    name: str
    state: str
    facility: str
    error: Optional[str] = None
    screenshot_path: Optional[str] = None


@dataclass
class MessageResult:
    """Result of attempting to send an eMessage."""
    success: bool
    contact_name: str
    subject: str
    error: Optional[str] = None
    screenshot_path: Optional[str] = None


@dataclass
class StampPurchaseResult:
    """Result of a stamp purchase attempt."""
    success: bool
    state: str
    package_size: int
    cost_usd: float
    error: Optional[str] = None
    screenshot_path: Optional[str] = None


STAMP_PACKAGES = [
    {"size": 6, "cost": 2.00},
    {"size": 20, "cost": 5.00},
    {"size": 35, "cost": 7.50},
    {"size": 60, "cost": 10.00},
]

AGENCY_TO_STATE = {
    "washington state department of corrections": "WA",
    "oklahoma department of corrections": "OK",
    "nys doccs inmate services": "NY",
    "california department of corrections & rehabilitation": "CA",
    "arkansas doc": "AR",
}


class SecurusClient:
    """
    Automates interactions with the Securus eMessaging platform.

    Usage:
        async with SecurusClient() as client:
            await client.login()
            result = await client.add_contact("John", "Doe", "WA", "Some Facility")
            result = await client.send_message("JOHN DOE", "Hello", "Message body")
    """

    MAIN_LOGIN_URL = "https://securustech.online/#/login"
    MY_ACCOUNT_URL = "https://securustech.online/#/my-account"
    DEBIT_CONTACTS_URL = "https://securustech.online/#/products/securus-debit/contacts"
    EMESSAGE_INBOX_URL = "https://securustech.online/#/products/emessage/inbox"

    def __init__(self, headless: bool = False):
        self._headless = headless
        self._playwright: Optional[Playwright] = None
        self._browser: Optional[Browser] = None
        self._context: Optional[BrowserContext] = None
        self._page: Optional[Page] = None
        self._logged_in = False
        self._last_action_time: float = 0
        self._messages_sent_this_hour: int = 0
        self._hour_start: float = time.time()

    # =========================================================================
    # LIFECYCLE
    # =========================================================================

    async def __aenter__(self) -> "SecurusClient":
        await self.start_browser()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()

    async def start_browser(self) -> None:
        """Launch browser with stealth settings."""
        log.info("Starting browser", headless=self._headless)
        self._playwright = await async_playwright().start()
        self._browser = await self._playwright.chromium.launch(
            headless=self._headless,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
            ],
        )
        self._context = await self._browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/131.0.0.0 Safari/537.36"
            ),
            locale="en-US",
            timezone_id="America/Chicago",
        )
        self._page = await self._context.new_page()
        if _USE_NEW_STEALTH:
            stealth = Stealth()
            await stealth.apply_stealth_async(self._page)
        else:
            await stealth_async(self._page)
        self._page.set_default_timeout(settings.browser_timeout)
        log.info("Browser started")

    async def close(self) -> None:
        """Shut down browser."""
        if self._page:
            await self._page.close()
        if self._context:
            await self._context.close()
        if self._browser:
            await self._browser.close()
        if self._playwright:
            await self._playwright.stop()
        self._page = None
        self._context = None
        self._browser = None
        self._playwright = None
        self._logged_in = False
        log.info("Browser closed")

    @property
    def page(self) -> Page:
        if self._page is None:
            raise RuntimeError("Browser not started")
        return self._page

    # =========================================================================
    # RATE LIMITING
    # =========================================================================

    async def _rate_limit(self) -> None:
        """Enforce delay between actions and hourly message cap."""
        now = time.time()
        elapsed = now - self._last_action_time
        min_delay = settings.securus_action_delay
        if elapsed < min_delay:
            wait = min_delay - elapsed + random.uniform(0.5, 2.0)
            log.debug("Rate limiting", wait=f"{wait:.1f}s")
            await asyncio.sleep(wait)
        self._last_action_time = time.time()

    def _check_hourly_cap(self) -> bool:
        """Check if we've hit the hourly message limit."""
        now = time.time()
        if now - self._hour_start > 3600:
            self._hour_start = now
            self._messages_sent_this_hour = 0
        return self._messages_sent_this_hour < settings.securus_max_messages_per_hour

    # =========================================================================
    # UTILITIES
    # =========================================================================

    async def _screenshot(self, name: str) -> str:
        """Take a debug screenshot."""
        SCREENSHOT_DIR.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        path = SCREENSHOT_DIR / f"{ts}_{name}.png"
        await self.page.screenshot(path=str(path), full_page=True)
        log.debug("Screenshot", path=str(path))
        return str(path)

    async def _dismiss_overlays(self) -> None:
        """Dismiss chat widgets, modals, popups, and purge stale overlay divs."""
        for sel in [".popup-close-button", "button:has-text('×')"]:
            try:
                btn = self.page.locator(sel).first
                if await btn.is_visible(timeout=500):
                    await btn.click()
                    await self.page.wait_for_timeout(300)
            except Exception:
                pass

        # Remove all .reveal-overlay divs from the DOM so they can't
        # intercept pointer events or cause strict-mode violations
        await self.page.evaluate(
            "document.querySelectorAll('.reveal-overlay').forEach(el => el.remove())"
        )

    async def _ensure_logged_in(self) -> None:
        """Re-login if session has expired."""
        if not self._logged_in:
            await self.login()
            return

        # Quick check: if we're on login page, session died
        if "/login" in self.page.url:
            log.warning("Session expired, re-logging in")
            self._logged_in = False
            await self.login()

    async def _human_delay(self, min_ms: int = 300, max_ms: int = 800) -> None:
        """Small random delay to mimic human behavior."""
        await self.page.wait_for_timeout(random.randint(min_ms, max_ms))

    # =========================================================================
    # LOGIN
    # =========================================================================

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=5, min=5, max=60),
        retry=retry_if_exception_type((PwTimeout, ConnectionError)),
    )
    async def login(self) -> bool:
        """
        Log in to Securus. Retries up to 3 times with exponential backoff.
        Returns True on success, raises on final failure.
        """
        log.info("Logging in to Securus")
        await self.page.goto(self.MAIN_LOGIN_URL, wait_until="domcontentloaded")
        await self.page.wait_for_timeout(5000)

        # Fill email
        email_field = self.page.locator("input[type='email']").first
        await email_field.wait_for(state="visible", timeout=10000)
        await email_field.click()
        await self._human_delay()
        await email_field.fill(settings.securus_email)
        await self._human_delay()

        # Fill password
        pw_field = self.page.locator("input[type='password']").first
        await pw_field.click()
        await self._human_delay()
        await pw_field.fill(settings.securus_password)
        await self._human_delay()

        # Submit
        await self.page.locator("button[type='submit']").first.click()
        log.info("Credentials submitted")

        # Wait for redirect away from login
        await self.page.wait_for_function(
            "() => !window.location.hash.includes('/login')",
            timeout=20000,
        )
        await self.page.wait_for_timeout(3000)

        # Verify
        if "/login" in self.page.url:
            ss = await self._screenshot("login_failed")
            raise ConnectionError(f"Login failed — still on login page: {self.page.url}")

        await self._dismiss_overlays()
        self._logged_in = True
        log.info("Login successful", url=self.page.url)
        return True

    # =========================================================================
    # LAUNCH EMESSAGING
    # =========================================================================

    async def launch_emessaging(self) -> None:
        """From the dashboard, click LAUNCH on the eMessaging section."""
        await self._ensure_logged_in()
        await self.page.goto(self.MY_ACCOUNT_URL, wait_until="domcontentloaded")
        await self.page.wait_for_timeout(3000)

        launch_btn = self.page.locator("a:has-text('LAUNCH')[href*='emessage'], a:has-text('Launch')").first
        await launch_btn.wait_for(state="visible", timeout=10000)
        await launch_btn.click()
        await self.page.wait_for_timeout(3000)
        log.info("Launched eMessaging", url=self.page.url)

    # =========================================================================
    # ADD CONTACT (via eMessaging Contacts)
    # =========================================================================

    async def add_contact(
        self,
        first_name: str,
        last_name: str,
        state: str,
        facility: str,
        inmate_id: Optional[str] = None,
    ) -> ContactResult:
        """
        Add an inmate as an eMessaging contact.

        Flow: eMessaging → Contacts → Add Contact → fill form → Search →
              click Add Contact on result → confirm popup.
        """
        await self._ensure_logged_in()
        await self._rate_limit()

        log.info("Adding eMessaging contact",
                 first_name=first_name, last_name=last_name,
                 state=state, inmate_id=inmate_id)

        try:
            # Navigate to eMessaging inbox, then Contacts
            await self.page.goto(self.EMESSAGE_INBOX_URL, wait_until="domcontentloaded")
            await self.page.wait_for_timeout(2000)
            await self._dismiss_overlays()

            await self.page.locator("text=Contacts").first.click()
            await self.page.wait_for_timeout(2000)

            # Click ADD CONTACT
            await self.page.locator("text=ADD CONTACT").first.click()
            await self.page.wait_for_timeout(2000)

            if inmate_id:
                # Switch to ID radio
                id_radio = self.page.locator("input[type='radio']").nth(1)
                await id_radio.click()
                await self._human_delay()

                visible_inputs = self.page.locator("input[type='text']:visible")
                await visible_inputs.first.fill(inmate_id)
            else:
                name_radio = self.page.locator("input[type='radio']").first
                await name_radio.click()
                await self._human_delay()

                visible_inputs = self.page.locator("input[type='text']:visible")
                await visible_inputs.nth(0).fill(first_name)
                await self._human_delay(200, 400)
                await visible_inputs.nth(1).fill(last_name)

            await self._human_delay()

            # Select State — find the first visible "Select" dropdown
            selects = self.page.locator("select:visible")
            sel_count = await selects.count()
            state_select = None

            for idx in range(sel_count):
                sel = selects.nth(idx)
                aria = await sel.get_attribute("aria-label") or ""
                if "navigation" in aria.lower():
                    continue
                first_opt = await sel.evaluate("s => s.options[0]?.text || ''")
                if first_opt == "Select":
                    state_select = sel
                    break

            if not state_select:
                ss = await self._screenshot("no_state_dropdown")
                return ContactResult(
                    success=False, inmate_id=inmate_id or "",
                    name=f"{first_name} {last_name}", state=state,
                    facility=facility, error="State dropdown not found",
                    screenshot_path=ss,
                )

            await state_select.select_option(label=state)
            log.info("State selected", state=state)
            await self.page.wait_for_timeout(2000)

            # Re-discover agency dropdown AFTER state selection (it loads dynamically)
            agency_select = None
            selects = self.page.locator("select:visible")
            sel_count = await selects.count()
            for idx in range(sel_count):
                sel = selects.nth(idx)
                aria = await sel.get_attribute("aria-label") or ""
                if "navigation" in aria.lower():
                    continue
                first_opt = await sel.evaluate("s => s.options[0]?.text || ''")
                if first_opt == "Select":
                    # Skip the state dropdown (already selected, won't show "Select")
                    selected_val = await sel.evaluate("s => s.options[s.selectedIndex]?.text || ''")
                    if selected_val == "Select":
                        agency_select = sel
                        break

            if agency_select:
                try:
                    await agency_select.select_option(label=facility, timeout=5000)
                    log.info("Agency selected", facility=facility)
                except PwTimeout:
                    options = await agency_select.evaluate(
                        "sel => Array.from(sel.options).map(o => ({value: o.value, text: o.text}))"
                    )
                    real_opts = [o for o in options if o["text"].strip().lower() != "select"]

                    matched = None
                    # 1) Exact substring match
                    for opt in real_opts:
                        if facility.lower() in opt["text"].lower():
                            matched = opt
                            break
                    # 2) Reverse substring (dropdown text in our facility string)
                    if not matched:
                        for opt in real_opts:
                            if opt["text"].lower() in facility.lower():
                                matched = opt
                                break
                    # 3) If only one real option, use it
                    if not matched and len(real_opts) == 1:
                        matched = real_opts[0]

                    if matched:
                        await agency_select.select_option(value=matched["value"])
                        log.info("Matched agency", matched=matched["text"])
                    else:
                        available = [o["text"] for o in options[:20]]
                        ss = await self._screenshot("agency_not_found")
                        return ContactResult(
                            success=False, inmate_id=inmate_id or "",
                            name=f"{first_name} {last_name}", state=state,
                            facility=facility,
                            error=f"Agency not in dropdown. Available: {available}",
                            screenshot_path=ss,
                        )

            await self._human_delay()

            # Click SEARCH
            await self.page.locator("button:has-text('SEARCH')").first.click()
            log.info("Search submitted")
            await self.page.wait_for_timeout(3000)
            await self._screenshot("search_results")

            # Check for "CONTACT CANNOT BE FOUND" popup
            not_found_popup = self.page.locator("text=CONTACT CANNOT BE FOUND")
            try:
                await not_found_popup.wait_for(state="visible", timeout=2000)
                close_btn = self.page.locator("button:has-text('CLOSE'), a:has-text('CLOSE')").first
                await close_btn.click()
                await self.page.wait_for_timeout(1000)
                ss = await self._screenshot("contact_not_found_popup")
                return ContactResult(
                    success=False, inmate_id=inmate_id or "",
                    name=f"{first_name} {last_name}", state=state,
                    facility=facility,
                    error="Contact not found on Securus (service may not be available)",
                    screenshot_path=ss,
                )
            except PwTimeout:
                pass

            # Look for Add Contact button/link in the results
            add_btn = None
            for sel in [
                "button:has-text('ADD CONTACT')",
                "button:has-text('Add Contact')",
                "a:has-text('ADD CONTACT')",
                "a:has-text('Add Contact')",
                "button:has-text('ADD')",
                "button:has-text('Add')",
            ]:
                loc = self.page.locator(sel).first
                try:
                    if await loc.is_visible(timeout=2000):
                        add_btn = loc
                        break
                except Exception:
                    continue

            if not add_btn:
                body_text = await self.page.locator("body").text_content() or ""
                ss = await self._screenshot("no_add_button")
                if "no results" in body_text.lower() or "not found" in body_text.lower():
                    return ContactResult(
                        success=False, inmate_id=inmate_id or "",
                        name=f"{first_name} {last_name}", state=state,
                        facility=facility, error="No results found",
                        screenshot_path=ss,
                    )
                return ContactResult(
                    success=False, inmate_id=inmate_id or "",
                    name=f"{first_name} {last_name}", state=state,
                    facility=facility,
                    error="Search returned but no Add Contact button found",
                    screenshot_path=ss,
                )

            await add_btn.click()
            log.info("Clicked ADD CONTACT on search result")

            # Wait for CONFIRM CONTACT popup to appear
            try:
                confirm_header = self.page.locator("text=CONFIRM CONTACT")
                await confirm_header.wait_for(state="visible", timeout=5000)
                log.info("Confirm popup appeared")
                await self._screenshot("confirm_popup")

                # Click the ADD CONTACT button inside the popup (not the one behind it)
                # The popup has CANCEL and ADD CONTACT buttons
                popup_buttons = self.page.locator("button:visible")
                btn_count = await popup_buttons.count()
                confirmed = False
                for idx in range(btn_count):
                    btn = popup_buttons.nth(idx)
                    txt = (await btn.text_content() or "").strip()
                    if txt == "ADD CONTACT":
                        await btn.click()
                        confirmed = True
                        log.info("Clicked ADD CONTACT on confirm popup")
                        break

                if not confirmed:
                    ss = await self._screenshot("confirm_btn_not_found")
                    return ContactResult(
                        success=False, inmate_id=inmate_id or "",
                        name=f"{first_name} {last_name}", state=state,
                        facility=facility,
                        error="Confirm popup appeared but could not click ADD CONTACT",
                        screenshot_path=ss,
                    )

                await self.page.wait_for_timeout(3000)
            except PwTimeout:
                ss = await self._screenshot("no_confirm_popup")
                return ContactResult(
                    success=False, inmate_id=inmate_id or "",
                    name=f"{first_name} {last_name}", state=state,
                    facility=facility,
                    error="Confirm popup did not appear after clicking ADD CONTACT",
                    screenshot_path=ss,
                )

            await self._screenshot("after_add_contact")
            log.info("Contact added", name=f"{first_name} {last_name}")

            return ContactResult(
                success=True,
                inmate_id=inmate_id or "",
                name=f"{first_name} {last_name}",
                state=state,
                facility=facility,
            )

        except Exception as e:
            ss = await self._screenshot("add_contact_error")
            log.error("Failed to add contact", error=str(e),
                      name=f"{first_name} {last_name}")
            return ContactResult(
                success=False, inmate_id=inmate_id or "",
                name=f"{first_name} {last_name}", state=state,
                facility=facility, error=str(e), screenshot_path=ss,
            )

    # =========================================================================
    # SEND MESSAGE (via eMessaging Compose)
    # =========================================================================

    async def send_message(
        self,
        contact_name: str,
        subject: str,
        body: str,
    ) -> MessageResult:
        """
        Send an eMessage to an existing contact.

        The contact must already be in the eMessaging contacts list
        (i.e., added via add_contact first).

        Args:
            contact_name: Exact name as it appears in the contacts dropdown
            subject: Message subject line
            body: Message body text

        Returns:
            MessageResult with success/failure details
        """
        if not self._check_hourly_cap():
            return MessageResult(
                success=False,
                contact_name=contact_name,
                subject=subject,
                error=f"Hourly message cap reached ({settings.securus_max_messages_per_hour}/hr)",
            )

        await self._ensure_logged_in()
        await self._rate_limit()

        log.info("Sending message", contact=contact_name, subject=subject)

        try:
            # Navigate to eMessaging inbox and click Compose
            await self.page.goto(self.EMESSAGE_INBOX_URL, wait_until="domcontentloaded")
            await self.page.wait_for_timeout(3000)
            await self._dismiss_overlays()

            compose_tab = self.page.locator("text=Compose").first
            await compose_tab.wait_for(state="visible", timeout=10000)
            await compose_tab.click()
            await self.page.wait_for_timeout(2000)

            # Handle "DRAFT MESSAGE" modal + "DELETE CONFIRMATION" follow-up
            try:
                ok_btn = self.page.locator(
                    "button:has-text('OK'):visible, a:has-text('OK'):visible"
                ).first
                await ok_btn.wait_for(state="visible", timeout=3000)
                await ok_btn.click()
                log.info("Dismissed draft popup (clicked OK)")
                await self.page.wait_for_timeout(2000)

                try:
                    delete_btn = self.page.locator(
                        "button:has-text('DELETE'):visible"
                    ).first
                    await delete_btn.wait_for(state="visible", timeout=3000)
                    await delete_btn.click()
                    log.info("Dismissed delete confirmation (clicked DELETE)")
                    await self.page.wait_for_timeout(2000)
                except PwTimeout:
                    pass
            except PwTimeout:
                pass

            # Purge any overlay divs left behind before interacting with the form
            await self._dismiss_overlays()

            # Select contact from dropdown
            contact_select = self.page.locator("select#select-inmate, select[name='selectInmate']").first
            await contact_select.wait_for(state="visible", timeout=10000)

            # Try exact match first, then partial
            try:
                await contact_select.select_option(label=contact_name, timeout=3000)
            except PwTimeout:
                options = await contact_select.evaluate(
                    "sel => Array.from(sel.options).map(o => ({value: o.value, text: o.text}))"
                )
                matched = None
                for opt in options:
                    if contact_name.upper() in opt["text"].upper():
                        matched = opt
                        break
                if matched:
                    await contact_select.select_option(value=matched["value"])
                    log.info("Matched contact", matched=matched["text"])
                else:
                    available = [o["text"] for o in options if o["text"] != "Select"]
                    ss = await self._screenshot("contact_not_in_dropdown")
                    return MessageResult(
                        success=False,
                        contact_name=contact_name,
                        subject=subject,
                        error=f"Contact '{contact_name}' not found in dropdown. "
                              f"Available: {available[:10]}",
                        screenshot_path=ss,
                    )

            await self._human_delay()

            # Check for "EMESSAGING NOT AVAILABLE" popup after contact selection
            try:
                not_avail = self.page.locator("text=NOT AVAILABLE").first
                await not_avail.wait_for(state="visible", timeout=2000)
                ok_btn = self.page.locator("button:has-text('OK'):visible").first
                await ok_btn.click()
                await self.page.wait_for_timeout(1000)
                await self._dismiss_overlays()
                ss = await self._screenshot("emessaging_not_available")
                return MessageResult(
                    success=False,
                    contact_name=contact_name,
                    subject=subject,
                    error="eMessaging not available at this contact's location",
                    screenshot_path=ss,
                )
            except PwTimeout:
                pass

            # Fill subject — use type() to trigger Angular validation
            subject_input = self.page.locator("input#subject, input[name='subject']").first
            try:
                await subject_input.wait_for(state="visible", timeout=5000)
            except PwTimeout:
                log.warning("Subject input not found by ID, trying label")
                subject_label = self.page.locator("label:has-text('Subject')")
                subject_for = await subject_label.get_attribute("for")
                if subject_for:
                    subject_input = self.page.locator(f"#{subject_for}")

            await subject_input.click()
            await self._human_delay(200, 400)
            await subject_input.fill("")
            await subject_input.type(subject, delay=15)
            await subject_input.dispatch_event("input")
            await subject_input.dispatch_event("change")
            await subject_input.dispatch_event("blur")
            await self._human_delay()

            # Fill message body — fill + dispatch events for Angular
            message_input = self.page.locator(
                "textarea#message, textarea[name='message'], "
                "textarea[formcontrolname='message']"
            ).first
            try:
                await message_input.wait_for(state="visible", timeout=5000)
            except PwTimeout:
                msg_label = self.page.locator("label:has-text('Compose Message')")
                msg_for = await msg_label.get_attribute("for")
                if msg_for:
                    message_input = self.page.locator(f"#{msg_for}")

            await message_input.click()
            await self._human_delay(200, 400)
            await message_input.fill(body)
            await message_input.dispatch_event("input")
            await message_input.dispatch_event("change")
            await message_input.dispatch_event("blur")
            await self._human_delay()

            await self._screenshot("message_composed")

            # Click SEND — wait for it to become enabled
            send_btn = self.page.locator(
                "button:has-text('SEND'), button:has-text('Send'), "
                "input[type='submit']:has-text('Send')"
            ).first
            await send_btn.wait_for(state="visible", timeout=5000)
            try:
                await send_btn.click(timeout=10000)
            except PwTimeout:
                # If still disabled, force click
                log.warning("Send button still disabled, force-clicking")
                await send_btn.click(force=True)
            log.info("Send button clicked")
            await self.page.wait_for_timeout(2000)

            # Handle STAMP USAGE confirmation popup
            try:
                stamp_header = self.page.locator("text=STAMP USAGE")
                await stamp_header.wait_for(state="visible", timeout=5000)
                log.info("Stamp usage popup appeared")
                await self._screenshot("stamp_usage_popup")

                confirm_btn = self.page.locator("button:has-text('CONFIRM')").first
                await confirm_btn.click()
                log.info("Clicked CONFIRM on stamp usage popup")
                await self.page.wait_for_timeout(3000)
            except PwTimeout:
                log.info("No stamp usage popup appeared, continuing")

            # Verify we actually left the Compose screen
            await self.page.wait_for_timeout(2000)
            current_url = self.page.url
            page_text = await self.page.locator("body").text_content() or ""

            # Success indicators: redirected to inbox/sent, or compose form is gone
            compose_still_visible = False
            try:
                subj = self.page.locator("input#subject, input[name='subject']").first
                compose_still_visible = await subj.is_visible(timeout=1000)
            except Exception:
                pass

            has_error = False
            for err_sel in [".error-message", ".alert-danger", "text=error"]:
                try:
                    err_el = self.page.locator(err_sel).first
                    if await err_el.is_visible(timeout=500):
                        has_error = True
                        break
                except Exception:
                    pass

            if compose_still_visible or has_error:
                ss = await self._screenshot("send_failed_still_on_compose")
                err_msg = "Message may not have sent — still on compose screen"
                if has_error:
                    err_msg = "Error detected after clicking send"
                log.warning(err_msg, url=current_url)
                return MessageResult(
                    success=False,
                    contact_name=contact_name,
                    subject=subject,
                    error=err_msg,
                    screenshot_path=ss,
                )

            await self._screenshot("message_sent")
            self._messages_sent_this_hour += 1
            log.info("Message sent successfully",
                     contact=contact_name,
                     msgs_this_hour=self._messages_sent_this_hour)

            return MessageResult(
                success=True,
                contact_name=contact_name,
                subject=subject,
            )

        except Exception as e:
            ss = await self._screenshot("send_message_error")
            log.error("Failed to send message", error=str(e), contact=contact_name)
            return MessageResult(
                success=False,
                contact_name=contact_name,
                subject=subject,
                error=str(e),
                screenshot_path=ss,
            )

    # =========================================================================
    # CONTACT LISTING
    # =========================================================================

    async def list_emessaging_contacts(self) -> list[dict]:
        """
        Get the list of current eMessaging contacts.
        Returns list of dicts with 'name', 'id', 'site' keys.
        """
        await self._ensure_logged_in()
        await self._rate_limit()

        log.info("Listing eMessaging contacts")
        await self.page.goto(self.EMESSAGE_INBOX_URL, wait_until="domcontentloaded")
        await self.page.wait_for_timeout(4000)
        await self._dismiss_overlays()

        # Click Contacts tab
        contacts_tab = self.page.locator("span:has-text('Contacts'), a:has-text('Contacts')").first
        await contacts_tab.click()
        await self.page.wait_for_timeout(3000)

        # Extract contacts from table
        contacts = []
        rows = self.page.locator("table tbody tr")
        count = await rows.count()

        for i in range(count):
            row = rows.nth(i)
            cells = row.locator("td")
            cell_count = await cells.count()
            if cell_count >= 3:
                name = (await cells.nth(0).text_content() or "").strip()
                cid = (await cells.nth(1).text_content() or "").strip()
                site = (await cells.nth(2).text_content() or "").strip()
                if name:
                    contacts.append({"name": name, "id": cid, "site": site})

        log.info("Contacts retrieved", count=len(contacts))
        return contacts

    async def get_compose_contacts(self) -> list[dict]:
        """
        Get the list of contacts available in the Compose dropdown.
        Returns list of dicts with 'value' (internal ID) and 'text' (name).
        """
        await self._ensure_logged_in()
        await self._rate_limit()

        await self.page.goto(self.EMESSAGE_INBOX_URL, wait_until="domcontentloaded")
        await self.page.wait_for_timeout(4000)
        await self._dismiss_overlays()

        compose_tab = self.page.locator("span:has-text('Compose'), a:has-text('Compose')").first
        await compose_tab.click()
        await self.page.wait_for_timeout(3000)

        # Handle draft modal
        try:
            ok_btn = self.page.locator(".reveal button:has-text('OK'), button:has-text('OK')").first
            if await ok_btn.is_visible(timeout=2000):
                await ok_btn.click()
                await self.page.wait_for_timeout(1000)
        except Exception:
            pass

        contact_select = self.page.locator("select#select-inmate, select[name='selectInmate']").first
        await contact_select.wait_for(state="visible", timeout=10000)

        options = await contact_select.evaluate(
            "sel => Array.from(sel.options).map(o => ({value: o.value, text: o.text}))"
        )
        # Filter out the placeholder "Select" option
        contacts = [o for o in options if o["value"]]
        log.info("Compose contacts retrieved", count=len(contacts))
        return contacts

    # =========================================================================
    # STAMP PURCHASING
    # =========================================================================

    @staticmethod
    def _pick_package(deficit: int) -> dict:
        """Return the smallest stamp package that covers the deficit."""
        for pkg in STAMP_PACKAGES:
            if pkg["size"] >= deficit:
                return pkg
        return STAMP_PACKAGES[-1]

    async def _navigate_to_purchase_stamps(self) -> bool:
        """Navigate to the Purchase Stamps page via my-account. Returns True on success."""
        await self._ensure_logged_in()
        await self.page.goto(self.MY_ACCOUNT_URL, wait_until="domcontentloaded")
        await self.page.wait_for_timeout(3000)
        await self._dismiss_overlays()

        purchase_link = self.page.locator("a:has-text('Purchase Stamps')").first
        try:
            await purchase_link.wait_for(state="visible", timeout=10000)
            await purchase_link.click()
            await self.page.wait_for_timeout(3000)
            await self._dismiss_overlays()
            log.info("Navigated to Purchase Stamps page")
            return True
        except PwTimeout:
            ss = await self._screenshot("purchase_stamps_link_not_found")
            log.error("Purchase Stamps link not found on my-account page",
                      screenshot=ss)
            return False

    async def get_stamp_balances(self) -> dict[str, int]:
        """
        Read per-state stamp balances from the Purchase Stamps dropdown.

        Parses optgroup labels or option text matching the pattern
        "<Agency Name>: <N> Stamps Available" and maps agency names
        to state abbreviations.
        """
        await self._rate_limit()

        if not await self._navigate_to_purchase_stamps():
            return {}

        await self._screenshot("stamp_balances_page")

        balances: dict[str, int] = {}

        # Extract dropdown structure: try optgroups first, fall back to options
        dropdown_data = await self.page.evaluate("""
            () => {
                const sel = document.querySelector('select');
                if (!sel) return null;

                const groups = [];
                const optgroups = sel.querySelectorAll('optgroup');
                optgroups.forEach(g => {
                    groups.push({
                        label: g.label || '',
                        options: Array.from(g.options).map(o => ({
                            value: o.value, text: o.text
                        }))
                    });
                });

                const allOptions = Array.from(sel.options).map(o => ({
                    value: o.value, text: o.text
                }));

                return { optgroups: groups, options: allOptions };
            }
        """)

        if not dropdown_data:
            log.warning("No dropdown found on Purchase Stamps page")
            return balances

        stamp_pattern = re.compile(r"(\d+)\s+Stamps?\s+Available", re.IGNORECASE)

        # Parse optgroup labels (preferred)
        if dropdown_data.get("optgroups"):
            for group in dropdown_data["optgroups"]:
                label = group.get("label", "")
                match = stamp_pattern.search(label)
                if match:
                    count = int(match.group(1))
                    label_lower = label.lower()
                    for agency, state in AGENCY_TO_STATE.items():
                        if agency in label_lower:
                            balances[state] = balances.get(state, 0) + count
                            break

        # Fall back to option text if no optgroups found
        if not balances and dropdown_data.get("options"):
            for opt in dropdown_data["options"]:
                text = opt.get("text", "")
                match = stamp_pattern.search(text)
                if match:
                    count = int(match.group(1))
                    text_lower = text.lower()
                    for agency, state in AGENCY_TO_STATE.items():
                        if agency in text_lower:
                            balances[state] = balances.get(state, 0) + count
                            break

        log.info("Stamp balances parsed", balances=balances)
        return balances

    async def purchase_stamps(
        self,
        state: str,
        package_size: int,
        agency_name: str,
    ) -> StampPurchaseResult:
        """
        Buy a stamp package for a given state.

        Navigates to Purchase Stamps, selects a contact from the target
        state's facility, picks the package, and completes checkout.
        Retries up to 3 times if clicking "Next" causes a logout.
        """
        pkg = next((p for p in STAMP_PACKAGES if p["size"] == package_size), None)
        if not pkg:
            return StampPurchaseResult(
                success=False, state=state, package_size=package_size,
                cost_usd=0, error=f"Invalid package size: {package_size}",
            )

        MAX_PURCHASE_ATTEMPTS = 3

        for attempt in range(1, MAX_PURCHASE_ATTEMPTS + 1):
            log.info("Attempting stamp purchase",
                     state=state, package=package_size, attempt=attempt)

            try:
                await self._ensure_logged_in()
                await self._rate_limit()

                if not await self._navigate_to_purchase_stamps():
                    return StampPurchaseResult(
                        success=False, state=state, package_size=package_size,
                        cost_usd=0,
                        error="Could not navigate to Purchase Stamps page",
                    )

                # ── Select a contact from the target state ──
                contact_dropdown = self.page.locator("select").first
                await contact_dropdown.wait_for(state="visible", timeout=10000)

                # Find an option belonging to the target state's agency
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

                target_option = None
                agency_lower = agency_name.lower()
                for opt in dropdown_info:
                    if not opt["value"]:
                        continue
                    group_lower = opt.get("group", "").lower()
                    text_lower = opt.get("text", "").lower()
                    if agency_lower in group_lower or agency_lower in text_lower:
                        target_option = opt
                        break

                if not target_option:
                    available = [f"{o['group']} / {o['text']}" for o in dropdown_info
                                 if o["value"]][:10]
                    ss = await self._screenshot("stamp_no_contact_for_state")
                    return StampPurchaseResult(
                        success=False, state=state, package_size=package_size,
                        cost_usd=0,
                        error=f"No contact found for {agency_name}. "
                              f"Available: {available}",
                        screenshot_path=ss,
                    )

                await contact_dropdown.select_option(value=target_option["value"])
                log.info("Selected contact for stamp purchase",
                         contact=target_option["text"], state=state)
                await self.page.wait_for_timeout(2000)

                # ── Select the stamp package ──
                await self._screenshot("stamp_packages_visible")

                # Find the radio/checkbox for the target package by matching
                # text like "6 Stamps Package" or "60 Stamps Package"
                pkg_label = f"{package_size} Stamps Package"
                pkg_locator = self.page.locator(f"text='{pkg_label}'").first
                try:
                    await pkg_locator.wait_for(state="visible", timeout=5000)
                    await pkg_locator.click()
                    log.info("Package selected", package=pkg_label)
                except PwTimeout:
                    # Try clicking the radio input near the package text
                    radio = self.page.locator(
                        f"input[type='radio']:near(:text('{pkg_label}'))"
                    ).first
                    try:
                        await radio.click(timeout=5000)
                        log.info("Package radio clicked", package=pkg_label)
                    except PwTimeout:
                        ss = await self._screenshot("stamp_package_not_found")
                        return StampPurchaseResult(
                            success=False, state=state,
                            package_size=package_size, cost_usd=0,
                            error=f"Could not find/select package: {pkg_label}",
                            screenshot_path=ss,
                        )

                await self._human_delay()

                # ── Click Next ──
                next_btn = self.page.locator(
                    "button:has-text('Next'), a:has-text('Next'), "
                    "button:has-text('NEXT'), a:has-text('NEXT')"
                ).first
                await next_btn.wait_for(state="visible", timeout=10000)
                await next_btn.click()
                log.info("Clicked Next on stamp purchase")
                await self.page.wait_for_timeout(4000)

                # ── Check for logout (known flaky behavior) ──
                if "/login" in self.page.url:
                    log.warning("Logged out after clicking Next",
                                attempt=attempt)
                    self._logged_in = False
                    if attempt < MAX_PURCHASE_ATTEMPTS:
                        continue
                    ss = await self._screenshot("stamp_purchase_logged_out")
                    return StampPurchaseResult(
                        success=False, state=state,
                        package_size=package_size, cost_usd=0,
                        error=f"Kept getting logged out after clicking Next "
                              f"({MAX_PURCHASE_ATTEMPTS} attempts)",
                        screenshot_path=ss,
                    )

                await self._screenshot("stamp_confirmation_page")

                # ── Click Buy / Submit / Confirm on the confirmation page ──
                buy_btn = None
                for sel in [
                    "button:has-text('Buy')", "button:has-text('BUY')",
                    "button:has-text('Submit')", "button:has-text('SUBMIT')",
                    "button:has-text('Confirm')", "button:has-text('CONFIRM')",
                    "button:has-text('Purchase')", "button:has-text('PURCHASE')",
                    "input[type='submit']",
                ]:
                    loc = self.page.locator(sel).first
                    try:
                        if await loc.is_visible(timeout=2000):
                            buy_btn = loc
                            break
                    except Exception:
                        continue

                if not buy_btn:
                    ss = await self._screenshot("stamp_no_buy_button")
                    return StampPurchaseResult(
                        success=False, state=state,
                        package_size=package_size, cost_usd=0,
                        error="Confirmation page loaded but Buy button not found",
                        screenshot_path=ss,
                    )

                await buy_btn.click()
                log.info("Clicked Buy on confirmation page")
                await self.page.wait_for_timeout(4000)

                # ── Check for logout again ──
                if "/login" in self.page.url:
                    log.warning("Logged out after clicking Buy", attempt=attempt)
                    self._logged_in = False
                    if attempt < MAX_PURCHASE_ATTEMPTS:
                        continue
                    ss = await self._screenshot("stamp_buy_logged_out")
                    return StampPurchaseResult(
                        success=False, state=state,
                        package_size=package_size, cost_usd=0,
                        error="Logged out after clicking Buy",
                        screenshot_path=ss,
                    )

                await self._screenshot("stamp_purchase_complete")
                log.info("Stamp purchase successful",
                         state=state, package=package_size,
                         cost=pkg["cost"])

                return StampPurchaseResult(
                    success=True, state=state,
                    package_size=package_size, cost_usd=pkg["cost"],
                )

            except Exception as e:
                log.error("Stamp purchase error",
                          error=str(e), state=state, attempt=attempt)
                if "/login" in self.page.url:
                    self._logged_in = False
                    if attempt < MAX_PURCHASE_ATTEMPTS:
                        continue
                ss = await self._screenshot("stamp_purchase_exception")
                return StampPurchaseResult(
                    success=False, state=state,
                    package_size=package_size, cost_usd=0,
                    error=str(e), screenshot_path=ss,
                )

        return StampPurchaseResult(
            success=False, state=state,
            package_size=package_size, cost_usd=0,
            error=f"Failed after {MAX_PURCHASE_ATTEMPTS} attempts",
        )
