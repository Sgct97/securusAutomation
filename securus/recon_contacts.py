"""
Final targeted recon — only explores contact-adding flows.

1. eMessaging "Contacts" tab (how contacts are managed for messaging)
2. Securus Debit "Find Contact" (properly click it and wait for form)

Read-only. Does not add contacts or send messages.
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

log = get_logger("securus.recon3")

SCREENSHOT_DIR = Path(__file__).resolve().parent.parent / "data" / "securus_recon3"
REPORT_PATH = SCREENSHOT_DIR / "recon3_report.json"
report_pages = []


async def ss(page: Page, name: str):
    SCREENSHOT_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%H%M%S")
    path = SCREENSHOT_DIR / f"{ts}_{name}.png"
    await page.screenshot(path=str(path), full_page=True)
    log.info("Screenshot", name=name)


async def dump(page: Page, label: str) -> dict:
    info = {"label": label, "url": page.url, "timestamp": datetime.now().isoformat()}

    info["all_visible_text"] = await page.evaluate("""() => {
        const main = document.querySelector('.grid-container, main, .content');
        if (main) return main.innerText?.substring(0, 3000) || '';
        return document.body.innerText?.substring(0, 3000) || '';
    }""")

    info["inputs"] = await page.evaluate("""() => {
        return Array.from(document.querySelectorAll('input, textarea, select'))
            .filter(el => el.offsetParent !== null)
            .map(el => ({tag: el.tagName, type: el.type||null, name: el.name||null,
                         id: el.id||null, placeholder: el.placeholder||null}));
    }""")

    info["buttons"] = await page.evaluate("""() => {
        return Array.from(document.querySelectorAll('button, input[type="submit"], a.button'))
            .filter(el => el.offsetParent !== null)
            .map(el => ({tag: el.tagName, text: (el.innerText||'').trim().substring(0,80),
                         id: el.id||null, classes: el.className||null, href: el.href||null}));
    }""")

    info["selects"] = await page.evaluate("""() => {
        return Array.from(document.querySelectorAll('select'))
            .filter(el => el.offsetParent !== null)
            .map(sel => ({
                name: sel.name||null, id: sel.id||null,
                options: Array.from(sel.options).slice(0,30).map(o => ({value: o.value, text: o.text}))
            }));
    }""")

    info["labels"] = await page.evaluate("""() => {
        return Array.from(document.querySelectorAll('label'))
            .filter(el => el.offsetParent !== null)
            .map(el => ({text: (el.innerText||'').trim().substring(0,80), for: el.htmlFor||null}));
    }""")

    info["forms"] = await page.evaluate("""() => {
        return Array.from(document.querySelectorAll('form'))
            .map(f => ({
                action: f.action||null, method: f.method||null, id: f.id||null,
                fields: Array.from(f.elements).map(el => ({
                    tag: el.tagName, type: el.type||null, name: el.name||null,
                    id: el.id||null, visible: el.offsetParent !== null
                }))
            }));
    }""")

    # Angular-specific: look for ng components that might be custom form elements
    info["angular_components"] = await page.evaluate("""() => {
        const selectors = [
            '[formcontrolname]', '[ngmodel]', 'mat-select', 'mat-input',
            'mat-form-field', 'ng-select', '[data-ng-model]', 'app-find-contact',
            '[class*="find-contact"]', '[class*="search"]'
        ];
        const results = [];
        for (const sel of selectors) {
            const els = document.querySelectorAll(sel);
            if (els.length > 0) {
                results.push({
                    selector: sel, count: els.length,
                    details: Array.from(els).slice(0,5).map(el => ({
                        tag: el.tagName, id: el.id||null, classes: el.className||null,
                        text: (el.innerText||'').trim().substring(0,100),
                        visible: el.offsetParent !== null,
                    }))
                });
            }
        }
        return results;
    }""")

    log.info("Page dump", label=label,
             inputs=len(info["inputs"]),
             buttons=len(info["buttons"]),
             selects=len(info["selects"]),
             forms=len(info["forms"]),
             angular=len(info["angular_components"]))
    report_pages.append(info)
    return info


async def dismiss_overlays(page: Page):
    """Dismiss chat widget and any modal overlays."""
    # Close chat
    for sel in [".popup-close-button", "button:has-text('×')"]:
        try:
            btn = page.locator(sel).first
            if await btn.is_visible(timeout=500):
                await btn.click()
                await page.wait_for_timeout(300)
        except Exception:
            pass

    # Close any reveal overlay / modal
    try:
        overlay = page.locator(".reveal-overlay:visible").first
        if await overlay.is_visible(timeout=500):
            close_btns = page.locator(".reveal-overlay button.close-button, .reveal-overlay a.close-button, .reveal button:has-text('OK'), .reveal button:has-text('Close')")
            count = await close_btns.count()
            for i in range(count):
                try:
                    await close_btns.nth(i).click()
                    await page.wait_for_timeout(500)
                except Exception:
                    pass
    except Exception:
        pass


async def login(page: Page) -> bool:
    log.info("Logging in...")
    await page.goto("https://securustech.online/#/login", wait_until="domcontentloaded")
    await page.wait_for_timeout(5000)

    try:
        email_field = page.locator("input[type='email']").first
        await email_field.wait_for(state="visible", timeout=10000)
        await email_field.click()
        await email_field.fill(settings.securus_email)
        await page.wait_for_timeout(500)

        pw_field = page.locator("input[type='password']").first
        await pw_field.click()
        await pw_field.fill(settings.securus_password)
        await page.wait_for_timeout(500)

        await page.locator("button[type='submit']").first.click()
        log.info("Login submitted, waiting for redirect...")

        await page.wait_for_function(
            "() => !window.location.hash.includes('/login')", timeout=20000)
        await page.wait_for_timeout(4000)

        # Verify we actually left the login page
        if "/login" in page.url:
            log.error("Still on login page after submit", url=page.url)
            await ss(page, "login_fail_still_on_login")
            return False

        await dismiss_overlays(page)
        await ss(page, "00_logged_in")
        log.info("Login confirmed", url=page.url)
        return True
    except Exception as e:
        log.error("Login failed", error=str(e))
        await ss(page, "login_fail")
        return False


async def explore_emessaging_contacts(page: Page):
    """Click the Contacts tab inside eMessaging and explore it."""
    log.info("=" * 50)
    log.info("EXPLORING: eMessaging Contacts tab")
    log.info("=" * 50)

    await page.goto("https://securustech.online/#/products/emessage/inbox",
                     wait_until="domcontentloaded")
    await page.wait_for_timeout(4000)
    await dismiss_overlays(page)
    await ss(page, "01_emessaging_inbox")

    # Click the "Contacts" tab in the sidebar
    contacts_tab = page.locator(".side-nav a:has-text('Contacts'), "
                                "a:has-text('Contacts'), "
                                "span:has-text('Contacts')").first
    try:
        await contacts_tab.wait_for(state="visible", timeout=5000)
        await contacts_tab.click()
        log.info("Clicked 'Contacts' tab in eMessaging sidebar")
    except PwTimeout:
        log.warning("Contacts tab not found with first selector set")
        # Try clicking by exact text within the eMessaging sidebar
        sidebar_items = page.locator("li, a, span").filter(has_text="Contacts")
        count = await sidebar_items.count()
        log.info("Elements containing 'Contacts'", count=count)
        for i in range(count):
            item = sidebar_items.nth(i)
            text = await item.text_content()
            tag = await item.evaluate("el => el.tagName")
            log.info("Contacts element", index=i, tag=tag, text=text.strip()[:50])
        if count > 0:
            await sidebar_items.first.click()
            log.info("Clicked first 'Contacts' element")

    await page.wait_for_timeout(3000)
    await dismiss_overlays(page)
    await ss(page, "02_emessaging_contacts")
    contacts_info = await dump(page, "emessaging_contacts")

    # Log the full visible text to understand the page
    log.info("Contacts page text", text=contacts_info["all_visible_text"][:2000])

    # Look for "Add Contact" or "New Contact" or "Find" buttons
    for sel_text in ["Add Contact", "New Contact", "Add", "Find", "Search", "Add Inmate"]:
        btn = page.locator(f"button:has-text('{sel_text}'), a:has-text('{sel_text}')").first
        try:
            if await btn.is_visible(timeout=800):
                log.info("Found add-contact button", text=sel_text)
                # Screenshot but DO NOT click
                await ss(page, f"03_found_btn_{sel_text.replace(' ', '_').lower()}")
        except Exception:
            pass

    # Get the full page HTML of just the main content area
    content_html = await page.evaluate("""() => {
        // Try to find the main content area (not nav/header/footer)
        const candidates = document.querySelectorAll(
            '.emessage-contacts, [class*="contact"], [class*="emessage"], ' +
            '.grid-container .cell, .content-area, main'
        );
        for (const el of candidates) {
            if (el.innerHTML.length > 100 && el.innerHTML.length < 20000) {
                return el.innerHTML;
            }
        }
        return document.querySelector('.off-canvas-content')?.innerHTML?.substring(0, 10000) || '';
    }""")
    log.info("Contacts content HTML (first 4000)", html=content_html[:4000])


async def explore_debit_find_contact(page: Page):
    """Properly explore the Securus Debit Find Contact form."""
    log.info("=" * 50)
    log.info("EXPLORING: Securus Debit Find Contact")
    log.info("=" * 50)

    await page.goto("https://securustech.online/#/products/securus-debit/contacts",
                     wait_until="domcontentloaded")
    await page.wait_for_timeout(3000)
    await dismiss_overlays(page)
    await ss(page, "04_debit_contacts_page")

    # Get the sidebar tab elements
    sidebar_html = await page.evaluate("""() => {
        const sidebar = document.querySelector('.side-nav, .sidebar, nav, [class*="side"]');
        if (sidebar) return sidebar.outerHTML;
        return '';
    }""")
    log.info("Sidebar HTML", html=sidebar_html[:2000])

    # Try multiple ways to click "Find Contact"
    clicked = False

    # Approach 1: Click by text within sidebar
    try:
        find_link = page.get_by_text("Find Contact", exact=True).first
        if await find_link.is_visible(timeout=2000):
            await find_link.click(force=True)
            log.info("Clicked 'Find Contact' via get_by_text (force=True)")
            clicked = True
    except Exception as e:
        log.warning("Approach 1 failed", error=str(e))

    if not clicked:
        # Approach 2: Click the first sidebar item (Find Contact is the first tab)
        try:
            first_tab = page.locator(".side-nav li:first-child a, .side-nav a").first
            text = await first_tab.text_content()
            log.info("First sidebar item", text=text.strip())
            await first_tab.click(force=True)
            log.info("Clicked first sidebar item")
            clicked = True
        except Exception as e:
            log.warning("Approach 2 failed", error=str(e))

    if not clicked:
        # Approach 3: JavaScript click
        try:
            await page.evaluate("""() => {
                const links = document.querySelectorAll('a, li');
                for (const link of links) {
                    if (link.innerText?.trim() === 'Find Contact') {
                        link.click();
                        return true;
                    }
                }
                return false;
            }""")
            log.info("Clicked 'Find Contact' via JavaScript")
            clicked = True
        except Exception as e:
            log.warning("Approach 3 failed", error=str(e))

    await page.wait_for_timeout(3000)
    await dismiss_overlays(page)
    await ss(page, "05_find_contact_form")
    form_info = await dump(page, "find_contact_form")

    log.info("Find Contact page text", text=form_info["all_visible_text"][:2000])
    log.info("Find Contact forms", forms=json.dumps(form_info["forms"], indent=2))
    log.info("Find Contact angular components",
             angular=json.dumps(form_info["angular_components"], indent=2))

    # Get full inner HTML of the content area
    content_html = await page.evaluate("""() => {
        const content = document.querySelector('.off-canvas-content');
        if (content) {
            // Find the main content section (not header/footer)
            const sections = content.querySelectorAll('.grid-container');
            for (const sec of sections) {
                const text = sec.innerText || '';
                if (text.includes('Find') || text.includes('Contact') || text.includes('Search')) {
                    return sec.innerHTML.substring(0, 8000);
                }
            }
        }
        return document.body.innerHTML.substring(0, 8000);
    }""")
    log.info("Content HTML (first 5000)", html=content_html[:5000])


async def main():
    SCREENSHOT_DIR.mkdir(parents=True, exist_ok=True)

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
        )
        stealth = Stealth()
        page = await context.new_page()
        await stealth.apply_stealth_async(page)
        page.set_default_timeout(15000)

        if not await login(page):
            await browser.close()
            return

        await explore_emessaging_contacts(page)
        await explore_debit_find_contact(page)

        with open(REPORT_PATH, "w") as f:
            json.dump({"pages": report_pages}, f, indent=2, default=str)
        log.info("Report saved")

        log.info("Done — browser stays open 15s")
        await page.wait_for_timeout(15000)
        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
