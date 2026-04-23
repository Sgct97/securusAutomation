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

# Reverse mapping used during add_contact: given our scraped inmate's
# state code, what's the DEFAULT agency name to pick in Securus' Agency
# dropdown when our stored facility name (e.g. "JACKIE BRANNON
# CORRECTIONAL CENTER") doesn't match any dropdown option (Securus only
# exposes one or two statewide "umbrella" agencies per state, not every
# physical prison). This prevented all 354 historical OK add_contact
# attempts from succeeding — our code kept returning "Agency not in
# dropdown" because none of our facility strings matched the two OK
# agency options. Substring/reverse-substring matches still run first
# so state-specific deviations (e.g. a future county jail) are still
# reachable; this map is the fallback.
STATE_TO_AGENCY_HINT = {
    "OK": "oklahoma department of corrections",
    "WA": "washington state department of corrections",
    "NY": "nys doccs inmate services",
    "CA": "california department of corrections & rehabilitation",
    "AR": "arkansas doc",
}

# Securus' State dropdown uses full state names, so pipeline.py passes e.g.
# "Oklahoma" to add_contact. But internal checks (OK leading-zero strip,
# STATE_TO_AGENCY_HINT lookup) key off the 2-letter code. Without this
# normalization those code paths silently never fire for any call made
# with a full-name state, which is every production call from the pipeline.
_STATE_FULL_TO_CODE = {
    "oklahoma": "OK",
    "washington": "WA",
    "new york": "NY",
    "california": "CA",
    "arkansas": "AR",
}


def _normalize_state_code(state: str) -> str:
    """Return the canonical 2-letter state code for *state*.

    Accepts either a code ("OK") or a full name ("Oklahoma"), case-insensitive.
    Returns uppercase code on match, otherwise the original input uppercased
    (so unmapped states still behave like before).
    """
    if not state:
        return ""
    key = state.strip().lower()
    if key in _STATE_FULL_TO_CODE:
        return _STATE_FULL_TO_CODE[key]
    return state.strip().upper()


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
    EMESSAGE_CONTACTS_URL = "https://securustech.online/#/products/emessage/contacts"
    STAMPS_TOTAL_URL = "https://securustech.online/#/products/emessage/stamps/totalStamps"
    STAMPS_PURCHASE_URL = "https://securustech.online/#/products/emessage/stamps/purchase"

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
            try:
                await self._page.close()
            except Exception:
                pass
        if self._context:
            try:
                await self._context.close()
            except Exception:
                pass
        if self._browser:
            try:
                await self._browser.close()
            except Exception:
                pass
        if self._playwright:
            try:
                await self._playwright.stop()
            except Exception:
                pass
        self._page = None
        self._context = None
        self._browser = None
        self._playwright = None
        self._logged_in = False
        log.info("Browser closed")

    async def relaunch_browser(self) -> None:
        """Tear down and re-launch the browser context from scratch.

        Used by the pipeline's circuit breaker when a session appears dead
        (repeated silent logouts / TimeoutErrors). A full re-launch clears
        Playwright's browser process, cookies, and any ThreatMetrix
        fingerprint state tied to the previous context. The caller is
        responsible for re-authenticating afterwards (``login()``) — we do
        NOT auto-login here because the caller may want to wait/cool-down
        first.
        """
        log.warning("Relaunching browser (circuit-breaker recovery)")
        await self.close()
        await self.start_browser()
        log.info("Browser relaunched")

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

    async def _detect_insufficient_stamps(self) -> Optional[str]:
        """
        After a contact is selected on the Compose page, detect whether
        Securus is blocking the send due to 0 stamps at that facility.

        Securus reacts to the "0 Stamps Available" state by:
          - Disabling the subject <input>
          - Optionally showing an "Insufficient Stamps" popup
          - Rendering text like "<Facility>: 0 Stamps Available"

        Returns the facility name (str) if a shortage is detected,
        otherwise None. Fast — intentionally uses short timeouts so we
        don't add latency to the happy path.
        """
        # Path 1: explicit "Insufficient Stamps" modal
        try:
            modal = self.page.locator("text=Insufficient Stamps").first
            await modal.wait_for(state="visible", timeout=1500)
            body_text = (await self.page.locator("body").text_content()) or ""
            # Pull out the facility name from the "<fac>: 0 Stamps Available"
            # pattern if it's there.
            m = re.search(r"([^\n]{3,120}?):\s*0\s*Stamps?\s*Available",
                          body_text, re.IGNORECASE)
            facility = m.group(1).strip() if m else "unknown facility"
            log.warning("Insufficient stamps detected via modal",
                        facility=facility)
            # Try to dismiss so subsequent retries start clean.
            for btn_sel in ["button:has-text('Cancel')",
                            "button:has-text('Close')",
                            "button:has-text('OK')"]:
                try:
                    b = self.page.locator(btn_sel).first
                    if await b.is_visible(timeout=500):
                        await b.click()
                        await self.page.wait_for_timeout(500)
                        break
                except Exception:
                    continue
            return facility
        except PwTimeout:
            pass

        # Path 2: no modal yet, but the subject input is already disabled
        # AND the page shows a zero-balance marker.
        try:
            subject_disabled = await self.page.evaluate(
                """() => {
                    const el = document.querySelector(
                        "input#subject, input[name='subject']");
                    return !!(el && el.disabled);
                }""")
        except Exception:
            subject_disabled = False

        if subject_disabled:
            body_text = (await self.page.locator("body").text_content()) or ""
            if "0 Stamps Available" in body_text or "until you have purchased stamps" in body_text:
                m = re.search(r"([^\n]{3,120}?):\s*0\s*Stamps?\s*Available",
                              body_text, re.IGNORECASE)
                facility = m.group(1).strip() if m else "unknown facility"
                log.warning("Insufficient stamps detected via disabled subject",
                            facility=facility)
                return facility

        return None

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
        max_attempts: int = 3,
    ) -> ContactResult:
        """
        Add an inmate as an eMessaging contact, retrying on mid-flow logouts.

        Retries up to *max_attempts* on logout/exception. Definitive failures
        (contact not found, agency missing, etc.) return immediately — no
        point retrying those.
        """
        # Prewarm the session before entering the real add-contact flow.
        # purchase_stamps gained this step and stopped seeing mid-flow
        # logouts; add_contact previously jumped straight to /emessage/inbox
        # and was getting its session killed ~2 minutes in (empirically
        # verified: add_contact for Douglas Simpson died at the "ADD
        # CONTACT" button click on all 3 attempts at the ~2 min mark on a
        # 30 s cool-off). Prewarm visits /contacts and /inbox first to
        # mimic a human browsing path and raise our ThreatMetrix score.
        # Best-effort: never raises.
        try:
            await self.prewarm_session()
        except Exception as e:
            log.warning("Prewarm before add_contact failed (continuing)",
                        error=str(e))

        last_result: Optional[ContactResult] = None
        for attempt in range(1, max(1, max_attempts) + 1):
            log.info("add_contact attempt",
                     attempt=attempt, max_attempts=max_attempts,
                     name=f"{first_name} {last_name}", state=state)
            result = await self._add_contact_once(
                first_name=first_name, last_name=last_name,
                state=state, facility=facility, inmate_id=inmate_id,
            )
            if result.success:
                return result
            last_result = result

            err = (result.error or "").lower()
            # Definitive: don't retry these
            if any(m in err for m in (
                "contact not found on securus",
                "no results found",
                "agency not in dropdown",
                "state dropdown not found",
                "no state dropdown",
            )):
                return result

            if attempt < max_attempts:
                log.warning("add_contact transient failure, retrying",
                            attempt=attempt, error=result.error)
                self._logged_in = False  # force re-login on next goto
                await asyncio.sleep(random.uniform(3, 6))

        return last_result or ContactResult(
            success=False, inmate_id=inmate_id or "",
            name=f"{first_name} {last_name}", state=state,
            facility=facility, error="add_contact exhausted retries",
        )

    async def _add_contact_once(
        self,
        first_name: str,
        last_name: str,
        state: str,
        facility: str,
        inmate_id: Optional[str] = None,
    ) -> ContactResult:
        """
        Single-attempt implementation of the add-contact flow.

        Flow: eMessaging → Contacts → Add Contact → fill form → Search →
              click Add Contact on result → confirm popup.
        """
        await self._ensure_logged_in()
        await self._rate_limit()

        log.info("Adding eMessaging contact",
                 first_name=first_name, last_name=last_name,
                 state=state, inmate_id=inmate_id)

        try:
            # Navigate to eMessaging inbox via the re-login-safe helper.
            # Raw page.goto would leave us stranded on /login if the session
            # dies mid-navigation (a common Securus behavior).
            await self._goto_or_relogin(self.EMESSAGE_INBOX_URL, max_retries=3)
            await self.page.wait_for_timeout(2000)
            await self._dismiss_overlays()

            await self.page.locator("text=Contacts").first.click()
            await self.page.wait_for_timeout(2000)

            # Click ADD CONTACT
            await self.page.locator("text=ADD CONTACT").first.click()
            await self.page.wait_for_timeout(2000)

            if inmate_id:
                # Per-state ID normalization before submitting to Securus.
                # Our scraper stores OK DOC IDs zero-padded to 10 chars
                # (e.g. "0000134726"), but Securus' OK DOC agency search
                # rejects that padding — it only accepts the trimmed form
                # ("134726"). This mismatch silently failed all 354 OK
                # add_contact attempts on the droplet before this fix.
                # Confirmed against the live Securus UI: searching Douglas
                # Simpson by the stripped ID finds him; the padded form
                # does not.
                # All other states (CA/WA/AR/NY) are already stored in
                # their native Securus format (CA "CC8032", WA "893117",
                # AR "186001", NY "26R0001"), so we only strip for OK.
                search_id = inmate_id
                if _normalize_state_code(state) == "OK":
                    stripped = inmate_id.lstrip("0")
                    if stripped and stripped != inmate_id:
                        log.info("Stripped OK ID leading zeros",
                                 raw=inmate_id, stripped=stripped)
                        search_id = stripped

                # Switch to ID radio
                id_radio = self.page.locator("input[type='radio']").nth(1)
                await id_radio.click()
                await self._human_delay()

                visible_inputs = self.page.locator("input[type='text']:visible")
                await visible_inputs.first.fill(search_id)
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
                    # 1) Exact substring match (our facility contains dropdown text)
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
                    # 3) State-level agency hint — covers states where our
                    #    stored facility name (e.g. "Jackie Brannon
                    #    Correctional Center") doesn't match either Securus
                    #    agency option ("Oklahoma Department of Corrections"
                    #    / "CHEROKEE COUNTY JAIL, OK"). Previously this
                    #    caused every OK add_contact to fail.
                    if not matched:
                        hint = STATE_TO_AGENCY_HINT.get(
                            _normalize_state_code(state))
                        if hint:
                            for opt in real_opts:
                                if hint in opt["text"].lower():
                                    matched = opt
                                    log.info("Matched agency via "
                                             "STATE_TO_AGENCY_HINT",
                                             state=state, hint=hint,
                                             matched=opt["text"])
                                    break
                    # 4) If only one real option, use it
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
        max_attempts: int = 3,
    ) -> MessageResult:
        """
        Send an eMessage to an existing contact, retrying on mid-flow logouts.

        Retries up to *max_attempts* on logout/exception. Definitive failures
        (insufficient stamps at facility, contact missing from dropdown, cap
        reached) return immediately — retrying them just burns session.
        """
        # Hourly cap is a hard definitive stop; skip the retry loop.
        if not self._check_hourly_cap():
            return MessageResult(
                success=False,
                contact_name=contact_name,
                subject=subject,
                error=f"Hourly message cap reached ({settings.securus_max_messages_per_hour}/hr)",
            )

        last_result: Optional[MessageResult] = None
        for attempt in range(1, max(1, max_attempts) + 1):
            log.info("send_message attempt",
                     attempt=attempt, max_attempts=max_attempts,
                     contact=contact_name)
            result = await self._send_message_once(
                contact_name=contact_name, subject=subject, body=body,
            )
            if result.success:
                return result
            last_result = result

            err = (result.error or "").lower()
            # Definitive: don't retry these. Retrying any of these just forces
            # another re-login (see self._logged_in=False below) and burns
            # ThreatMetrix score for no benefit.
            #
            # "emessaging not available at this contact's location" is the
            # error Securus raises when the contact is present but blocked
            # by their facility (the dropdown often shows the contact name
            # suffixed with " - BLOCKED"). On 2026-04-22 a single BLOCKED
            # contact (LYNDA MERCY) triggered 3 rapid re-login retries which
            # tripped Securus' bot detection; after that point every login
            # for the remaining 13 hours silently failed (0/857 succeeded),
            # the pipeline flailed through 91 more candidates, and only 4
            # of 25 targeted messages went out. Treating it as permanent
            # costs us nothing (the contact genuinely cannot receive mail
            # right now) and protects the session for the rest of the run.
            if any(m in err for m in (
                "insufficient stamps at facility",
                "contact not found in contacts dropdown",
                "hourly message cap reached",
                "emessaging not available",
            )):
                return result

            if attempt < max_attempts:
                log.warning("send_message transient failure, retrying",
                            attempt=attempt, error=result.error)
                self._logged_in = False  # force re-login on next goto
                await asyncio.sleep(random.uniform(3, 6))

        return last_result or MessageResult(
            success=False, contact_name=contact_name, subject=subject,
            error="send_message exhausted retries",
        )

    async def _send_message_once(
        self,
        contact_name: str,
        subject: str,
        body: str,
    ) -> MessageResult:
        """Single-attempt send flow. Retry wrapper is in send_message()."""
        await self._ensure_logged_in()
        await self._rate_limit()

        log.info("Sending message", contact=contact_name, subject=subject)

        try:
            # Navigate to eMessaging inbox via the re-login-safe helper
            # so a mid-flight logout retries rather than stranding us on
            # the login page.
            await self._goto_or_relogin(self.EMESSAGE_INBOX_URL, max_retries=3)
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

            # Try exact match first, then partial. Capture the label of the
            # option we actually selected so we can detect " - BLOCKED" (and
            # similar facility-block suffixes) before attempting to compose.
            selected_label: Optional[str] = None
            try:
                await contact_select.select_option(label=contact_name, timeout=3000)
                selected_label = contact_name
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
                    selected_label = matched["text"]
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

            # Short-circuit BLOCKED contacts: Securus annotates the dropdown
            # label with " - BLOCKED" when a contact exists but the facility
            # has disabled eMessaging for them. Continuing to the compose
            # form would trigger the "EMESSAGING NOT AVAILABLE" popup anyway;
            # detecting it here skips the wasted work and returns a
            # permanent-failure error that send_message() will NOT retry.
            if selected_label and "BLOCKED" in selected_label.upper():
                log.warning(
                    "Matched contact is BLOCKED at facility; skipping send",
                    contact_name=contact_name,
                    matched=selected_label,
                )
                return MessageResult(
                    success=False,
                    contact_name=contact_name,
                    subject=subject,
                    error=(
                        "eMessaging not available: contact is BLOCKED at "
                        f"facility (dropdown label='{selected_label}')"
                    ),
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

            # Detect "Insufficient Stamps" before attempting to compose — Securus
            # disables the subject input when there are 0 stamps at the contact's
            # facility. Without this check, the downstream subject-click would
            # time out for 30s and the whole outreach would be marked as a
            # generic Playwright failure.
            insufficient = await self._detect_insufficient_stamps()
            if insufficient:
                ss = await self._screenshot("insufficient_stamps")
                return MessageResult(
                    success=False,
                    contact_name=contact_name,
                    subject=subject,
                    error=(f"Insufficient stamps at facility: "
                           f"{insufficient}"),
                    screenshot_path=ss,
                )

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
        """Legacy fallback (static list). Prefer purchase_stamps(needed=...)
        which discovers real packages from the live page."""
        for pkg in STAMP_PACKAGES:
            if pkg["size"] >= deficit:
                return pkg
        return STAMP_PACKAGES[-1]

    async def _discover_stamp_packages(self) -> list[dict]:
        """
        Enumerate stamp-package radios rendered on the Purchase page after a
        contact is selected, and return ``[{size, cost, radio_id, label_text}]``
        sorted by size.

        The live page labels look like "500 Stamps ($ 5)" or "60 Stamps Package
        ($ 10.00)" depending on the facility, so we parse both forms with a
        single regex.
        """
        raw = await self.page.evaluate(r"""() => {
            const out = [];
            document.querySelectorAll('input[type="radio"]').forEach(el => {
                const cs = window.getComputedStyle(el);
                const visible = cs.display !== 'none'
                    && cs.visibility !== 'hidden';
                let label = null;
                if (el.id) {
                    const lbl = document.querySelector(
                        `label[for="${el.id}"]`);
                    if (lbl) label = lbl.textContent.trim();
                }
                if (!label && el.closest('label')) {
                    label = el.closest('label').textContent.trim();
                }
                out.push({
                    id: el.id || '',
                    name: el.name || '',
                    value: el.value || '',
                    visible,
                    disabled: el.disabled,
                    label: label || '',
                });
            });
            return out;
        }""")

        pkgs: list[dict] = []
        # Matches "500 Stamps ($ 5)", "60 Stamps Package ($ 10.00)",
        # "1000 Stamps($ 50)", etc.
        label_re = re.compile(
            r"(\d[\d,]*)\s*Stamps?(?:\s*Package)?\s*"
            r"\(\s*\$\s*([\d]+(?:\.\d+)?)\s*\)",
            re.IGNORECASE,
        )

        for r in raw:
            if not r["visible"] or r["disabled"]:
                continue
            m = label_re.search(r["label"])
            if not m:
                continue
            try:
                size = int(m.group(1).replace(",", ""))
                cost = float(m.group(2))
            except ValueError:
                continue
            if size <= 0:
                continue
            pkgs.append({
                "size": size,
                "cost": cost,
                "radio_id": r["id"],
                "label_text": r["label"],
            })

        pkgs.sort(key=lambda p: p["size"])
        return pkgs

    async def _goto_or_relogin(self, url: str, max_retries: int = 10) -> None:
        """Navigate to *url*. If the site redirects to login, re-authenticate
        and retry up to *max_retries* times. This is the single place that
        handles Securus' random session kills."""
        for attempt in range(1, max_retries + 1):
            await self.page.goto(url, wait_until="domcontentloaded")
            await self.page.wait_for_timeout(4000)
            await self._dismiss_overlays()

            if "/login" not in self.page.url:
                return  # success

            log.warning("Logged out during navigation, re-logging in",
                        target=url, attempt=attempt)
            self._logged_in = False
            await asyncio.sleep(random.uniform(3, 6))
            await self.login()

    async def _reset_to_fresh_purchase_page(self) -> None:
        """Force a clean load of the Stamps Purchase form.

        Securus' SPA keeps rendering whatever sub-view it last showed for a
        given hash route. After a successful purchase the page is left on
        the CONFIRMATION view of `#/products/emessage/stamps/purchase`
        ("Your payment using a credit card ending in XXXX is complete!
        FINISH"). A naive `goto(STAMPS_PURCHASE_URL)` right after that is a
        no-op from the router's perspective — the URL is already current
        and the confirmation view stays rendered, so the contact dropdown
        we need is never created. This caused every AR stamp purchase to
        burn all 15 retries in 2026-04-21's production run, screenshotting
        the stale WA confirmation page each time.

        We force a reset by routing through /inbox first. A different hash
        tears down the purchase view entirely; navigating back to
        /stamps/purchase then re-mounts a fresh form with the contact
        dropdown populated.
        """
        await self._goto_or_relogin(self.EMESSAGE_INBOX_URL, max_retries=3)
        await self.page.wait_for_timeout(1500)
        await self._goto_or_relogin(self.STAMPS_PURCHASE_URL)

    async def prewarm_session(self) -> None:
        """
        Visit Contacts and Inbox before touching money-moving pages.

        Empirical finding (see probe_output/logout_forensics_*.jsonl):
        Securus' ThreatMetrix scoring (valcontent.securustech.net/fp/HP)
        drops sessions that jump straight from login -> /stamps/purchase.
        A login -> /contacts -> /inbox -> /stamps/* path mirrors a real
        user and keeps the session alive.

        Best-effort: never raises; a logout mid-prewarm is tolerated.
        Call before ensure_stamps / add_contact / send_message loops.
        """
        for url in (self.EMESSAGE_CONTACTS_URL, self.EMESSAGE_INBOX_URL):
            try:
                await self._goto_or_relogin(url, max_retries=3)
                # Small idle so XHR/JS finishes like a human pausing.
                await self.page.wait_for_timeout(
                    random.randint(2500, 4500))
            except Exception as e:
                log.warning("Prewarm step failed (continuing)",
                            target=url, error=str(e))
        log.info("Session prewarm complete")

    async def get_stamp_balances(self) -> dict[str, int]:
        """
        Read per-state stamp balances from the Total Stamps page.

        Flow: navigate to Total Stamps URL → parse FACILITY / BALANCE table
        → map facility names to state codes.
        Re-logs in automatically if Securus kills the session.
        """
        await self._ensure_logged_in()
        await self._rate_limit()

        await self._goto_or_relogin(self.STAMPS_TOTAL_URL)
        await self._screenshot("stamp_balances_page")

        balances: dict[str, int] = {}

        # Parse only rows where the second cell is a plain integer
        rows = await self.page.evaluate(r"""
            () => {
                const rows = [];
                document.querySelectorAll('tr').forEach(tr => {
                    const cells = tr.querySelectorAll('td');
                    if (cells.length >= 2) {
                        const bal = cells[1].textContent.trim();
                        if (/^\d+$/.test(bal)) {
                            rows.push({
                                facility: cells[0].textContent.trim(),
                                balance: bal
                            });
                        }
                    }
                });
                return rows;
            }
        """)

        if not rows:
            log.warning("No stamp balance rows found on Total Stamps page")
            return balances

        for row in rows:
            facility = row.get("facility", "")
            try:
                count = int(row.get("balance", "0"))
            except ValueError:
                continue

            facility_lower = facility.lower()
            for agency, state in AGENCY_TO_STATE.items():
                if agency in facility_lower:
                    balances[state] = balances.get(state, 0) + count
                    break

        log.info("Stamp balances parsed", balances=balances, raw_rows=rows)
        return balances

    async def purchase_stamps(
        self,
        state: str,
        needed: int,
        contact_name: str,
        max_attempts: int = 15,
    ) -> StampPurchaseResult:
        """
        Buy enough stamps to cover ``needed`` sends for *state*, by selecting a
        known contact for that facility and choosing the smallest package on
        the live page that meets or exceeds ``needed``.

        Package sizes vary by facility (e.g. 6/20/35/60 vs CDCR's
        500/1000/2000/5000), so we discover available packages dynamically
        from the DOM after the contact is selected, rather than relying on a
        hardcoded list.

        The ``contact_name`` must already exist in the Securus contacts list
        (added via ``add_contact``). Retries on logout between steps.
        """
        if needed <= 0:
            return StampPurchaseResult(
                success=False, state=state, package_size=0, cost_usd=0,
                error=f"Invalid needed count: {needed}",
            )

        MAX_ATTEMPTS = max(1, max_attempts)

        chosen_size: int = 0
        chosen_cost: float = 0.0

        for attempt in range(1, MAX_ATTEMPTS + 1):
            log.info("Attempting stamp purchase",
                     state=state, needed=needed,
                     contact=contact_name, attempt=attempt)

            try:
                await self._ensure_logged_in()
                await self._rate_limit()

                # ── Navigate to a FRESH Purchase page ──
                # Route via /inbox first so the SPA tears down any stale
                # sub-view (e.g. the CONFIRMATION screen left over from a
                # previous purchase in the same run). Without this, the
                # router sees the same /stamps/purchase hash and skips
                # re-mounting the form, leaving us on the old confirmation
                # page with no contact dropdown. See
                # _reset_to_fresh_purchase_page docstring.
                await self._reset_to_fresh_purchase_page()

                # ── Find the contact dropdown (the one with many options) ──
                contact_dropdown = None
                selects = self.page.locator("select:visible")
                for i in range(await selects.count()):
                    sel = selects.nth(i)
                    opt_count = await sel.evaluate("s => s.options.length")
                    if opt_count > 50:
                        contact_dropdown = sel
                        break

                if not contact_dropdown:
                    ss = await self._screenshot("stamp_no_contact_dropdown")
                    if attempt < MAX_ATTEMPTS:
                        log.warning("Contact dropdown not found, retrying",
                                    attempt=attempt)
                        await asyncio.sleep(random.uniform(3, 6))
                        continue
                    return StampPurchaseResult(
                        success=False, state=state, package_size=0,
                        cost_usd=0,
                        error="Contact dropdown not found on Purchase page",
                        screenshot_path=ss,
                    )

                # Select the contact (exact match first, then partial)
                try:
                    await contact_dropdown.select_option(
                        label=contact_name, timeout=3000)
                except PwTimeout:
                    options = await contact_dropdown.evaluate(
                        "sel => Array.from(sel.options).map("
                        "o => ({value: o.value, text: o.text}))"
                    )
                    matched = None
                    cn_upper = contact_name.upper()
                    for opt in options:
                        if cn_upper in opt["text"].upper():
                            matched = opt
                            break
                    if matched:
                        await contact_dropdown.select_option(
                            value=matched["value"])
                    else:
                        ss = await self._screenshot("stamp_contact_not_in_list")
                        return StampPurchaseResult(
                            success=False, state=state,
                            package_size=0, cost_usd=0,
                            error=f"Contact '{contact_name}' not in dropdown",
                            screenshot_path=ss,
                        )

                log.info("Selected contact for stamp purchase",
                         contact=contact_name, state=state)
                await self.page.wait_for_timeout(3000)

                # ── Discover the stamp packages the live page is offering ──
                # Package sizes differ per facility (WA/OK/NY/AR: 6/20/35/60;
                # CDCR: 500/1000/2000/5000). Scrape the visible radios and
                # parse size/cost from each associated label.
                discovered = await self._discover_stamp_packages()
                if not discovered:
                    ss = await self._screenshot("stamp_no_packages_visible")
                    if attempt < MAX_ATTEMPTS:
                        log.warning(
                            "No stamp packages visible on page, retrying",
                            attempt=attempt,
                        )
                        await asyncio.sleep(random.uniform(3, 6))
                        continue
                    return StampPurchaseResult(
                        success=False, state=state, package_size=0,
                        cost_usd=0,
                        error="No stamp packages found on purchase page",
                        screenshot_path=ss,
                    )

                # Pick the smallest package that meets demand; if even the
                # largest falls short, fall back to the largest offered
                # (pipeline-level daily limit will throttle further buys).
                sorted_pkgs = sorted(discovered, key=lambda p: p["size"])
                selected = next(
                    (p for p in sorted_pkgs if p["size"] >= needed),
                    sorted_pkgs[-1],
                )
                chosen_size = selected["size"]
                chosen_cost = selected["cost"]
                log.info(
                    "Stamp package selected dynamically",
                    state=state, needed=needed,
                    chosen_size=chosen_size, chosen_cost=chosen_cost,
                    offered=[p["size"] for p in sorted_pkgs],
                )

                # Click the discovered radio. Prefer clicking its <label>
                # (bigger hit target, matches how a real user clicks), then
                # fall back to the input itself.
                clicked = False
                if selected.get("radio_id"):
                    label_loc = self.page.locator(
                        f"label[for='{selected['radio_id']}']"
                    ).first
                    try:
                        await label_loc.wait_for(state="visible", timeout=3000)
                        await label_loc.click()
                        clicked = True
                    except Exception:
                        pass
                    if not clicked:
                        radio_loc = self.page.locator(
                            f"input[type='radio']#{selected['radio_id']}"
                        ).first
                        try:
                            await radio_loc.click(timeout=3000)
                            clicked = True
                        except Exception:
                            pass
                if not clicked:
                    text_loc = self.page.locator(
                        f"text={selected['label_text']}"
                    ).first
                    await text_loc.click(timeout=5000)

                log.info("Package radio clicked",
                         size=chosen_size, cost=chosen_cost)

                await self._human_delay()

                # ── Click Next (known to randomly log out) ──
                next_btn = self.page.locator(
                    "button:has-text('Next'), a:has-text('Next'), "
                    "button:has-text('NEXT'), a:has-text('NEXT')"
                ).first
                await next_btn.wait_for(state="visible", timeout=10000)
                await next_btn.click()
                log.info("Clicked Next on stamp purchase")
                await self.page.wait_for_timeout(4000)

                if "/login" in self.page.url:
                    log.warning("Logged out after Next, retrying",
                                attempt=attempt)
                    self._logged_in = False
                    await asyncio.sleep(random.uniform(3, 6))
                    continue

                await self._screenshot("stamp_confirmation_page")

                # ── Click Submit on the confirmation page ──
                submit_btn = self.page.locator(
                    "button:has-text('Submit'), button:has-text('SUBMIT')"
                ).first
                try:
                    await submit_btn.wait_for(state="visible", timeout=5000)
                except PwTimeout:
                    for fallback in [
                        "button:has-text('Buy')", "button:has-text('BUY')",
                        "button:has-text('Confirm')",
                        "button:has-text('CONFIRM')",
                        "input[type='submit']",
                    ]:
                        loc = self.page.locator(fallback).first
                        try:
                            if await loc.is_visible(timeout=1500):
                                submit_btn = loc
                                break
                        except Exception:
                            continue
                    else:
                        ss = await self._screenshot("stamp_no_buy_button")
                        if attempt < MAX_ATTEMPTS:
                            log.warning("Buy button not found, retrying",
                                        attempt=attempt)
                            await asyncio.sleep(random.uniform(3, 6))
                            continue
                        return StampPurchaseResult(
                            success=False, state=state,
                            package_size=chosen_size, cost_usd=0,
                            error="Buy button not found on confirmation page",
                            screenshot_path=ss,
                        )

                await submit_btn.click()
                log.info("Clicked Submit on confirmation page")
                await self.page.wait_for_timeout(4000)

                if "/login" in self.page.url:
                    log.warning("Logged out after Submit, retrying",
                                attempt=attempt)
                    self._logged_in = False
                    await asyncio.sleep(random.uniform(3, 6))
                    continue

                await self._screenshot("stamp_purchase_complete")

                # ── Verify Securus actually accepted the payment ──
                # Securus does NOT change URL on failure — it just renders a
                # red alert banner on the same page saying the bank/card
                # rejected the charge (e.g. "We are unable to process your
                # request. Please ensure your billing address and credit card
                # information is correct."). Without this check the flow
                # would silently report success and the pipeline would then
                # think it has stamps it doesn't.
                #
                # We look for the distinctive phrases in visible page text.
                # Matching is case-insensitive and bounded by what Securus
                # actually renders; if Securus changes the copy we'll catch
                # it on the balance delta check downstream (pipeline always
                # re-reads balances before trusting a purchase).
                failure_phrases = [
                    "unable to process your request",
                    "billing address and credit card",
                    "contact your bank or credit card",
                    "payment was declined",
                    "transaction was declined",
                    "card was declined",
                ]
                try:
                    body_text = (await self.page.locator("body")
                                 .inner_text(timeout=3000)).lower()
                except Exception:
                    body_text = ""
                matched_phrase = next(
                    (p for p in failure_phrases if p in body_text), None)
                if matched_phrase:
                    ss = await self._screenshot("stamp_payment_declined")
                    log.error(
                        "Stamp purchase declined by payment processor",
                        state=state, package=chosen_size,
                        cost=chosen_cost, matched_phrase=matched_phrase,
                    )
                    # Definitive failure — do NOT retry. Retrying a declined
                    # card just hammers the processor and risks a hard block.
                    return StampPurchaseResult(
                        success=False, state=state,
                        package_size=chosen_size, cost_usd=0,
                        error=(
                            f"Payment declined by Securus/processor: "
                            f"matched='{matched_phrase}'. "
                            "No charge went through. Check billing address "
                            "and card on file before retrying."
                        ),
                        screenshot_path=ss,
                    )

                log.info("Stamp purchase successful",
                         state=state, package=chosen_size,
                         cost=chosen_cost)

                # Click FINISH to leave the confirmation view cleanly.
                # Belt-and-suspenders: _reset_to_fresh_purchase_page on the
                # NEXT call will handle this too, but clicking FINISH here
                # mirrors real user behavior and avoids leaving the session
                # parked on a payment-completion page any longer than
                # needed. Best-effort: a failure here doesn't invalidate
                # the successful purchase.
                try:
                    finish_btn = self.page.locator(
                        "button:has-text('Finish'), "
                        "button:has-text('FINISH'), "
                        "a:has-text('Finish'), a:has-text('FINISH')"
                    ).first
                    if await finish_btn.is_visible(timeout=2000):
                        await finish_btn.click()
                        await self.page.wait_for_timeout(1500)
                        log.info("Clicked FINISH on stamp confirmation")
                except Exception as e:
                    log.debug("FINISH click skipped (best-effort)",
                              error=str(e))

                return StampPurchaseResult(
                    success=True, state=state,
                    package_size=chosen_size, cost_usd=chosen_cost,
                )

            except Exception as e:
                log.error("Stamp purchase error",
                          error=str(e), state=state, attempt=attempt)
                self._logged_in = False
                if attempt < MAX_ATTEMPTS:
                    await asyncio.sleep(random.uniform(3, 6))
                    continue
                ss = await self._screenshot("stamp_purchase_exception")
                return StampPurchaseResult(
                    success=False, state=state,
                    package_size=chosen_size, cost_usd=0,
                    error=str(e), screenshot_path=ss,
                )

        return StampPurchaseResult(
            success=False, state=state,
            package_size=chosen_size, cost_usd=0,
            error=f"Failed after {MAX_ATTEMPTS} attempts",
        )
