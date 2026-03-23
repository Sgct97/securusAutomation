"""
Targeted Securus Recon — Phase 2

Only explores:
1. The "Find Contact" form under Securus Debit
2. The eMessaging "LAUNCH" page / compose flow

Does NOT navigate to purchase pages or external URLs.
"""

import asyncio
import json
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from playwright.async_api import async_playwright, Page, TimeoutError as PwTimeout
from playwright_stealth import Stealth
from config import settings
from logger import get_logger

log = get_logger("securus.recon2")

SCREENSHOT_DIR = Path(__file__).resolve().parent.parent / "data" / "securus_recon2"
REPORT_PATH = SCREENSHOT_DIR / "securus_recon2_report.json"

LOGIN_URL = settings.securus_login_url
EMAIL = settings.securus_email
PASSWORD = settings.securus_password

report_pages = []


async def ss(page: Page, name: str) -> str:
    SCREENSHOT_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%H%M%S")
    path = SCREENSHOT_DIR / f"{ts}_{name}.png"
    await page.screenshot(path=str(path), full_page=True)
    log.info("Screenshot", name=name)
    return str(path)


async def dump(page: Page, label: str) -> dict:
    info = {"label": label, "url": page.url, "title": await page.title(),
            "timestamp": datetime.now().isoformat()}

    info["buttons"] = await page.evaluate("""() => {
        return Array.from(document.querySelectorAll('button, a[role="button"], input[type="submit"]'))
            .filter(el => el.offsetParent !== null)
            .map(el => ({tag: el.tagName, text: (el.innerText||'').trim().substring(0,80),
                         id: el.id||null, classes: el.className||null}));
    }""")

    info["inputs"] = await page.evaluate("""() => {
        return Array.from(document.querySelectorAll('input, textarea, select'))
            .filter(el => el.offsetParent !== null)
            .map(el => ({tag: el.tagName, type: el.type||null, name: el.name||null,
                         id: el.id||null, placeholder: el.placeholder||null,
                         classes: el.className||null, label: null}));
    }""")

    info["links"] = await page.evaluate("""() => {
        return Array.from(document.querySelectorAll('a[href]'))
            .filter(el => el.offsetParent !== null)
            .map(el => ({text: (el.innerText||'').trim().substring(0,60), href: el.href||null}));
    }""")

    info["headings"] = await page.evaluate("""() => {
        return Array.from(document.querySelectorAll('h1,h2,h3,h4,h5'))
            .filter(el => el.offsetParent !== null)
            .map(el => ({tag: el.tagName, text: (el.innerText||'').trim().substring(0,100)}));
    }""")

    # Also grab all visible text labels near inputs
    info["labels"] = await page.evaluate("""() => {
        return Array.from(document.querySelectorAll('label, .mat-label, .form-label'))
            .filter(el => el.offsetParent !== null)
            .map(el => ({text: (el.innerText||'').trim().substring(0,80), for: el.htmlFor||null}));
    }""")

    # Grab select/dropdown options
    info["selects"] = await page.evaluate("""() => {
        return Array.from(document.querySelectorAll('select'))
            .filter(el => el.offsetParent !== null)
            .map(sel => ({
                name: sel.name||null, id: sel.id||null,
                options: Array.from(sel.options).slice(0,20).map(o => ({value: o.value, text: o.text}))
            }));
    }""")

    log.info("Page dump", label=label, buttons=len(info["buttons"]),
             inputs=len(info["inputs"]), labels=len(info["labels"]),
             selects=len(info["selects"]))
    report_pages.append(info)
    return info


async def dismiss_chat(page: Page):
    """Close the Securus chat widget if present."""
    for sel in ["button:has-text('×')", "[aria-label='Close']",
                ".chat-close", "button:has-text('Close')"]:
        try:
            btn = page.locator(sel).first
            if await btn.is_visible(timeout=500):
                await btn.click()
                await page.wait_for_timeout(300)
        except Exception:
            pass


async def login(page: Page) -> bool:
    log.info("Logging in...")
    await page.goto(LOGIN_URL, wait_until="domcontentloaded")
    await page.wait_for_timeout(4000)

    email_field = page.locator("input[type='email']").first
    try:
        await email_field.wait_for(state="visible", timeout=8000)
    except PwTimeout:
        log.error("Email field not found")
        await ss(page, "login_fail")
        return False

    await email_field.click()
    await email_field.fill(EMAIL)
    await page.wait_for_timeout(500)

    pw_field = page.locator("input[type='password']").first
    await pw_field.click()
    await pw_field.fill(PASSWORD)
    await page.wait_for_timeout(500)

    await page.locator("button[type='submit']").first.click()
    log.info("Login submitted")

    try:
        await page.wait_for_function(
            "() => !window.location.hash.includes('/login')", timeout=20000)
    except PwTimeout:
        log.error("Login failed — still on login page")
        await ss(page, "login_fail")
        return False

    await page.wait_for_timeout(3000)
    await dismiss_chat(page)
    log.info("Logged in", url=page.url)
    await ss(page, "00_logged_in")
    return True


async def explore_find_contact(page: Page):
    """Explore the Find Contact form under Securus Debit."""
    log.info("=" * 50)
    log.info("EXPLORING: Find Contact")
    log.info("=" * 50)

    await page.goto("https://securustech.online/#/products/securus-debit/contacts",
                     wait_until="domcontentloaded")
    await page.wait_for_timeout(3000)
    await dismiss_chat(page)
    await ss(page, "01_contacts_page")
    await dump(page, "contacts_page")

    # Click the "Find Contact" tab
    find_tab = page.locator("a:has-text('Find Contact'), button:has-text('Find Contact'), "
                            "div:has-text('Find Contact')").first
    try:
        await find_tab.wait_for(state="visible", timeout=5000)
        await find_tab.click()
        log.info("Clicked 'Find Contact' tab")
        await page.wait_for_timeout(3000)
    except PwTimeout:
        log.warning("'Find Contact' tab not found as link/button, trying text click")
        await page.get_by_text("Find Contact", exact=True).first.click()
        await page.wait_for_timeout(3000)

    await ss(page, "02_find_contact_form")
    form_info = await dump(page, "find_contact_form")

    # Log all form details
    log.info("Find Contact form inputs", inputs=json.dumps(form_info["inputs"], indent=2))
    log.info("Find Contact form labels", labels=json.dumps(form_info["labels"], indent=2))
    log.info("Find Contact form selects", selects=json.dumps(form_info["selects"], indent=2))
    log.info("Find Contact form buttons", buttons=json.dumps(form_info["buttons"], indent=2))

    # Check for any dropdowns that might be Angular Material / custom
    mat_selects = await page.evaluate("""() => {
        return Array.from(document.querySelectorAll('mat-select, [role="combobox"], [role="listbox"], .mat-select, .ng-select'))
            .filter(el => el.offsetParent !== null)
            .map(el => ({
                id: el.id||null, classes: el.className||null,
                text: (el.innerText||'').trim().substring(0,100),
                ariaLabel: el.getAttribute('aria-label')||null,
                placeholder: el.getAttribute('placeholder')||null,
            }));
    }""")
    log.info("Material/custom selects", selects=json.dumps(mat_selects, indent=2))

    # Get full form HTML for analysis
    form_html = await page.evaluate("""() => {
        const main = document.querySelector('main, .main-content, .content, app-root');
        if (main) return main.innerHTML.substring(0, 5000);
        return document.body.innerHTML.substring(0, 5000);
    }""")
    log.info("Page HTML snippet (first 3000 chars)", html=form_html[:3000])


async def explore_emessaging(page: Page):
    """Explore the eMessaging / compose flow."""
    log.info("=" * 50)
    log.info("EXPLORING: eMessaging")
    log.info("=" * 50)

    # First go back to My Account to find the LAUNCH button
    await page.goto("https://securustech.online/#/my-account",
                     wait_until="domcontentloaded")
    await page.wait_for_timeout(3000)
    await dismiss_chat(page)

    # Click LAUNCH button for eMessaging
    launch_btn = page.locator("a:has-text('LAUNCH'), button:has-text('LAUNCH')").first
    try:
        await launch_btn.wait_for(state="visible", timeout=5000)
        href = await launch_btn.get_attribute("href")
        log.info("LAUNCH button found", href=href)
        await launch_btn.click()
        log.info("Clicked LAUNCH")
    except PwTimeout:
        log.warning("LAUNCH button not found, trying direct URL")
        await page.goto("https://securustech.online/#/products/emessage",
                         wait_until="domcontentloaded")

    await page.wait_for_timeout(4000)
    await dismiss_chat(page)
    await ss(page, "03_emessaging_main")
    emsg_info = await dump(page, "emessaging_main")

    # Look for inbox/compose/contacts within eMESSAGING
    log.info("eMessaging page info", info=json.dumps(emsg_info, indent=2, default=str))

    # Try to find compose / new message button
    compose_selectors = [
        "a:has-text('Compose')", "button:has-text('Compose')",
        "a:has-text('New Message')", "button:has-text('New Message')",
        "a:has-text('Write')", "button:has-text('Write')",
        "a:has-text('Send')", "button:has-text('Send eMessage')",
        "[aria-label='Compose']", ".compose-btn", ".new-message",
    ]
    for sel in compose_selectors:
        try:
            loc = page.locator(sel).first
            if await loc.is_visible(timeout=800):
                log.info("Found compose element", selector=sel)
                await loc.click()
                await page.wait_for_timeout(3000)
                await ss(page, "04_compose_message")
                compose_info = await dump(page, "compose_message")
                log.info("Compose page info",
                         info=json.dumps(compose_info, indent=2, default=str))
                break
        except Exception:
            continue

    # Also get full page HTML for the messaging section
    msg_html = await page.evaluate("""() => {
        const main = document.querySelector('main, .main-content, .content, app-root');
        if (main) return main.innerHTML.substring(0, 8000);
        return document.body.innerHTML.substring(0, 8000);
    }""")
    log.info("eMessaging HTML snippet", html=msg_html[:4000])

    # Try navigating sidebar/tabs if present
    tabs = await page.evaluate("""() => {
        return Array.from(document.querySelectorAll('.nav-link, .tab, [role="tab"], .sidebar a, .side-nav a'))
            .filter(el => el.offsetParent !== null)
            .map(el => ({text: (el.innerText||'').trim(), href: el.href||null, classes: el.className||null}));
    }""")
    log.info("Messaging tabs/nav", tabs=json.dumps(tabs, indent=2))

    for tab in tabs:
        tab_text = tab.get("text", "")
        if not tab_text:
            continue
        lower = tab_text.lower()
        if any(kw in lower for kw in ["compose", "new", "write", "send", "draft", "outbox"]):
            log.info("Clicking messaging tab", tab=tab_text)
            try:
                await page.get_by_text(tab_text, exact=True).first.click()
                await page.wait_for_timeout(3000)
                await ss(page, f"05_{tab_text.replace(' ', '_').lower()[:20]}")
                await dump(page, f"msg_tab_{tab_text}")
            except Exception as e:
                log.warning("Failed to click tab", tab=tab_text, error=str(e))


async def main():
    SCREENSHOT_DIR.mkdir(parents=True, exist_ok=True)

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=False,
            args=["--no-sandbox", "--disable-dev-shm-usage",
                   "--disable-blink-features=AutomationControlled"],
        )
        context = await browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent=("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/131.0.0.0 Safari/537.36"),
            locale="en-US", timezone_id="America/Chicago",
        )
        stealth = Stealth()
        page = await context.new_page()
        await stealth.apply_stealth_async(page)
        page.set_default_timeout(15000)

        if not await login(page):
            await browser.close()
            return

        await explore_find_contact(page)
        await explore_emessaging(page)

        # Save report
        report = {
            "completed_at": datetime.now().isoformat(),
            "pages": report_pages,
        }
        with open(REPORT_PATH, "w") as f:
            json.dump(report, f, indent=2, default=str)
        log.info("Report saved", path=str(REPORT_PATH))

        log.info("Done — browser stays open 20s for inspection")
        await page.wait_for_timeout(20000)
        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
