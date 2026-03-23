"""
Securus eMessaging Platform — Reconnaissance Script

Opens a visible browser, logs in, and systematically explores every
section of the Securus interface. Screenshots are taken at each step
and a JSON report of discovered elements/flows is saved.

Run: venv/bin/python securus/recon.py
"""

import asyncio
import json
import os
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from playwright.async_api import async_playwright, Page, TimeoutError as PwTimeout
from playwright_stealth import Stealth
from config import settings
from logger import get_logger

log = get_logger("securus.recon")

SCREENSHOT_DIR = Path(__file__).resolve().parent.parent / "data" / "securus_recon"
REPORT_PATH = SCREENSHOT_DIR / "securus_recon_report.json"

LOGIN_URL = settings.securus_login_url  # https://securustech.online/#/login
EMAIL = settings.securus_email
PASSWORD = settings.securus_password


async def screenshot(page: Page, name: str) -> str:
    """Take a screenshot and return the file path."""
    SCREENSHOT_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%H%M%S")
    path = SCREENSHOT_DIR / f"{ts}_{name}.png"
    await page.screenshot(path=str(path), full_page=True)
    log.info("Screenshot saved", name=name, path=str(path))
    return str(path)


async def dump_page_info(page: Page, label: str) -> dict:
    """Extract structured info about the current page state."""
    info = {
        "label": label,
        "url": page.url,
        "title": await page.title(),
        "timestamp": datetime.now().isoformat(),
    }

    # Collect all visible buttons
    buttons = await page.evaluate("""() => {
        return Array.from(document.querySelectorAll('button, a[role="button"], input[type="submit"]'))
            .filter(el => el.offsetParent !== null)
            .map(el => ({
                tag: el.tagName,
                text: el.innerText?.trim().substring(0, 80),
                id: el.id || null,
                classes: el.className || null,
                type: el.type || null,
                href: el.href || null,
            }));
    }""")
    info["buttons"] = buttons

    # Collect all visible inputs
    inputs = await page.evaluate("""() => {
        return Array.from(document.querySelectorAll('input, textarea, select'))
            .filter(el => el.offsetParent !== null)
            .map(el => ({
                tag: el.tagName,
                type: el.type || null,
                name: el.name || null,
                id: el.id || null,
                placeholder: el.placeholder || null,
                classes: el.className || null,
            }));
    }""")
    info["inputs"] = inputs

    # Collect nav links / menu items
    nav_items = await page.evaluate("""() => {
        return Array.from(document.querySelectorAll('nav a, .nav a, [role="navigation"] a, .menu a, .sidebar a, a[routerlink], a[href*="#/"]'))
            .filter(el => el.offsetParent !== null)
            .map(el => ({
                text: el.innerText?.trim().substring(0, 60),
                href: el.href || null,
                classes: el.className || null,
            }));
    }""")
    info["nav_items"] = nav_items

    # Collect headings
    headings = await page.evaluate("""() => {
        return Array.from(document.querySelectorAll('h1, h2, h3'))
            .filter(el => el.offsetParent !== null)
            .map(el => ({
                tag: el.tagName,
                text: el.innerText?.trim().substring(0, 100),
            }));
    }""")
    info["headings"] = headings

    log.info("Page info collected",
             label=label,
             buttons=len(buttons),
             inputs=len(inputs),
             nav_items=len(nav_items))
    return info


async def handle_popups(page: Page):
    """Dismiss any modal popups, cookie banners, etc."""
    dismiss_selectors = [
        "button:has-text('OK')",
        "button:has-text('Accept')",
        "button:has-text('Close')",
        "button:has-text('Got it')",
        "button:has-text('Dismiss')",
        ".modal button.close",
        "[aria-label='Close']",
        "button:has-text('I Understand')",
    ]
    for sel in dismiss_selectors:
        try:
            btn = page.locator(sel).first
            if await btn.is_visible(timeout=500):
                await btn.click()
                await page.wait_for_timeout(500)
                log.info("Dismissed popup", selector=sel)
        except Exception:
            pass


async def login(page: Page) -> bool:
    """Attempt to log in to Securus. Returns True on success."""
    log.info("Navigating to login page", url=LOGIN_URL)
    await page.goto(LOGIN_URL, wait_until="domcontentloaded")
    await page.wait_for_timeout(5000)

    await handle_popups(page)
    await screenshot(page, "01_login_page")
    login_info = await dump_page_info(page, "login_page")

    # Find email/username field — try multiple strategies
    email_field = None
    email_selectors = [
        "input[type='email']",
        "input[name='email']",
        "input[name='username']",
        "input[placeholder*='mail' i]",
        "input[placeholder*='user' i]",
        "input[formcontrolname='email']",
        "input[formcontrolname='username']",
        "input[autocomplete='email']",
        "input[autocomplete='username']",
    ]
    for sel in email_selectors:
        loc = page.locator(sel).first
        try:
            if await loc.is_visible(timeout=1000):
                email_field = loc
                log.info("Email field found", selector=sel)
                break
        except Exception:
            continue

    if not email_field:
        log.warning("Email field not found by specific selectors, trying all visible inputs")
        await screenshot(page, "01b_email_field_fallback")
        inputs = page.locator("input:visible")
        count = await inputs.count()
        log.info("Visible inputs found", count=count)
        for i in range(count):
            inp = inputs.nth(i)
            inp_type = await inp.get_attribute("type") or "text"
            inp_name = await inp.get_attribute("name") or ""
            inp_placeholder = await inp.get_attribute("placeholder") or ""
            log.info("Input", index=i, type=inp_type, name=inp_name, placeholder=inp_placeholder)
        if count == 0:
            log.error("No visible inputs on login page")
            return False
        # Use first non-password, non-hidden input
        for i in range(count):
            inp = inputs.nth(i)
            inp_type = await inp.get_attribute("type") or "text"
            if inp_type not in ("password", "hidden", "submit", "button", "checkbox"):
                email_field = inp
                break
        if not email_field:
            email_field = inputs.first

    await email_field.click()
    await page.wait_for_timeout(200)
    await email_field.fill(EMAIL)
    log.info("Email entered", email=EMAIL)
    await page.wait_for_timeout(800)

    # Find password field
    pw_field = page.locator("input[type='password']").first
    try:
        await pw_field.wait_for(state="visible", timeout=5000)
    except PwTimeout:
        log.error("Password field not found")
        await screenshot(page, "01c_no_password_field")
        return False

    await pw_field.click()
    await page.wait_for_timeout(200)
    await pw_field.fill(PASSWORD)
    log.info("Password entered")
    await page.wait_for_timeout(800)
    await screenshot(page, "02_credentials_filled")

    # Find and click login/sign-in button
    login_btn = None
    btn_selectors = [
        "button[type='submit']",
        "button:has-text('Log In')",
        "button:has-text('Login')",
        "button:has-text('LOG IN')",
        "button:has-text('Sign In')",
        "button:has-text('Sign in')",
        "button:has-text('SIGN IN')",
        "input[type='submit']",
        "a:has-text('Log In')",
        "a:has-text('Sign In')",
    ]
    for sel in btn_selectors:
        loc = page.locator(sel).first
        try:
            if await loc.is_visible(timeout=800):
                login_btn = loc
                log.info("Login button found", selector=sel)
                break
        except Exception:
            continue

    if not login_btn:
        log.error("Login button not found")
        await screenshot(page, "02b_no_login_button")
        login_info_post = await dump_page_info(page, "no_login_button")
        return False

    await login_btn.click()
    log.info("Login button clicked — waiting for response")

    # Wait for URL to change away from /login, or for new content
    try:
        await page.wait_for_function(
            "() => !window.location.hash.includes('/login')",
            timeout=20000,
        )
        log.info("URL changed away from login page")
    except PwTimeout:
        log.warning("URL did not change from login — checking for errors or 2FA")

    await page.wait_for_timeout(3000)
    await handle_popups(page)
    await screenshot(page, "03_after_login")

    # Check for login failure indicators
    error_texts = await page.evaluate("""() => {
        const els = document.querySelectorAll(
            '.error, .alert-danger, [role="alert"], .toast-error, .text-danger, ' +
            '.mat-error, .error-message, .login-error, .notification-error'
        );
        return Array.from(els).map(el => el.innerText?.trim()).filter(Boolean);
    }""")
    if error_texts:
        log.error("Login errors detected", errors=error_texts)
        await screenshot(page, "03b_login_errors")
        return False

    # Check if still on login page
    if "/login" in page.url.lower():
        log.warning("Still on login page after submit — possible failure or 2FA")
        await screenshot(page, "03c_still_on_login")
        post_info = await dump_page_info(page, "post_login_still_on_login")
        log.info("Post-login page info", info=post_info)
        return False

    log.info("Login appears successful", url=page.url)
    return True


async def explore_dashboard(page: Page) -> dict:
    """Explore the post-login dashboard."""
    await handle_popups(page)
    await screenshot(page, "04_dashboard")
    info = await dump_page_info(page, "dashboard")
    return info


async def explore_section(page: Page, nav_text: str, nav_href: str, idx: int) -> dict:
    """Navigate to a section and explore it."""
    label = nav_text.replace(" ", "_").lower()[:30]
    log.info("Exploring section", section=nav_text, href=nav_href)

    try:
        if nav_href and nav_href.startswith("http"):
            await page.goto(nav_href, wait_until="domcontentloaded")
        else:
            link = page.locator(f"a:has-text('{nav_text}')").first
            if await link.is_visible(timeout=3000):
                await link.click()
            else:
                log.warning("Nav link not visible, trying href navigation", href=nav_href)
                if nav_href:
                    await page.goto(nav_href, wait_until="domcontentloaded")
                else:
                    return {"label": nav_text, "error": "link not visible and no href"}

        await page.wait_for_timeout(3000)
        await handle_popups(page)

        ss_name = f"05_{idx:02d}_{label}"
        await screenshot(page, ss_name)
        info = await dump_page_info(page, f"section_{label}")
        return info

    except Exception as e:
        log.error("Failed to explore section", section=nav_text, error=str(e))
        await screenshot(page, f"05_{idx:02d}_{label}_error")
        return {"label": nav_text, "error": str(e)}


async def run_recon():
    """Main recon flow."""
    report = {
        "started_at": datetime.now().isoformat(),
        "login_url": LOGIN_URL,
        "email": EMAIL,
        "pages": [],
    }

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=False,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
            ],
        )
        context = await browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/131.0.0.0 Safari/537.36"
            ),
            locale="en-US",
            timezone_id="America/Chicago",
        )
        stealth = Stealth()
        page = await context.new_page()
        await stealth.apply_stealth_async(page)
        page.set_default_timeout(30000)

        # ── Step 1: Login ──
        log.info("=" * 60)
        log.info("SECURUS RECON — Starting")
        log.info("=" * 60)

        success = await login(page)
        if not success:
            log.error("Login failed — aborting recon")
            report["login_success"] = False
            report["completed_at"] = datetime.now().isoformat()
            save_report(report)
            await browser.close()
            return report

        report["login_success"] = True

        # ── Step 2: Dashboard exploration ──
        dashboard_info = await explore_dashboard(page)
        report["pages"].append(dashboard_info)

        # ── Step 3: Explore every nav section ──
        nav_items = dashboard_info.get("nav_items", [])
        log.info("Found nav items to explore", count=len(nav_items))

        visited_hrefs = set()
        for idx, item in enumerate(nav_items):
            href = item.get("href", "")
            text = item.get("text", "")
            if not text or href in visited_hrefs:
                continue
            visited_hrefs.add(href)

            section_info = await explore_section(page, text, href, idx)
            report["pages"].append(section_info)

            # If this looks like contacts/connections, do deeper exploration
            lower_text = text.lower()
            if any(kw in lower_text for kw in ["contact", "connection", "inmate", "add"]):
                log.info("Deep-exploring contact-related section", section=text)
                deep_info = await dump_page_info(page, f"deep_{text}")
                await screenshot(page, f"06_deep_{text.replace(' ', '_').lower()[:20]}")
                report["pages"].append(deep_info)

            # If this looks like messaging, do deeper exploration
            if any(kw in lower_text for kw in ["message", "mail", "compose", "inbox"]):
                log.info("Deep-exploring messaging section", section=text)
                deep_info = await dump_page_info(page, f"deep_{text}")
                await screenshot(page, f"07_deep_{text.replace(' ', '_').lower()[:20]}")
                report["pages"].append(deep_info)

            await page.wait_for_timeout(1500)

        # ── Step 4: Try direct URL navigation to key sections ──
        base = "https://securustech.online/#"
        direct_urls = [
            (f"{base}/home", "direct_home"),
            (f"{base}/dashboard", "direct_dashboard"),
            (f"{base}/contacts", "direct_contacts"),
            (f"{base}/connections", "direct_connections"),
            (f"{base}/messages", "direct_messages"),
            (f"{base}/inbox", "direct_inbox"),
            (f"{base}/compose", "direct_compose"),
            (f"{base}/emessaging", "direct_emessaging"),
            (f"{base}/settings", "direct_settings"),
            (f"{base}/account", "direct_account"),
        ]

        log.info("Trying direct URL navigation to key sections")
        for url, label in direct_urls:
            try:
                await page.goto(url, wait_until="domcontentloaded")
                await page.wait_for_timeout(3000)
                await handle_popups(page)

                if "/login" not in page.url.lower():
                    ss_path = await screenshot(page, f"08_{label}")
                    info = await dump_page_info(page, label)
                    report["pages"].append(info)
                    log.info("Direct URL accessible", url=url, actual_url=page.url)
                else:
                    log.info("Direct URL redirected to login", url=url)
            except Exception as e:
                log.debug("Direct URL failed", url=url, error=str(e))

        report["completed_at"] = datetime.now().isoformat()
        save_report(report)

        log.info("=" * 60)
        log.info("RECON COMPLETE")
        log.info("=" * 60)
        log.info("Screenshots saved to", dir=str(SCREENSHOT_DIR))
        log.info("Report saved to", path=str(REPORT_PATH))

        # Keep browser open so user can inspect
        log.info("Browser will stay open for 30s for manual inspection...")
        await page.wait_for_timeout(30000)
        await browser.close()

    return report


def save_report(report: dict):
    """Save the recon report to JSON."""
    SCREENSHOT_DIR.mkdir(parents=True, exist_ok=True)
    with open(REPORT_PATH, "w") as f:
        json.dump(report, f, indent=2, default=str)
    log.info("Report saved", path=str(REPORT_PATH))


if __name__ == "__main__":
    asyncio.run(run_recon())
