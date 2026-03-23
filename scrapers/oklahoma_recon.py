"""
Oklahoma DOC Inmate Search Reconnaissance Script (v2)

Updated based on initial findings:
1. Must click "Accept" button on homepage first
2. Must navigate to "Search" page via nav link
3. Search page has a simple reCAPTCHA checkbox

This script investigates the full workflow.
"""

import asyncio
import json
from datetime import datetime
from pathlib import Path
import random

from playwright.async_api import async_playwright


BASE_URL = "https://okoffender.doc.ok.gov/"
OUTPUT_DIR = Path(__file__).parent.parent / "data"


async def human_delay(min_ms: int = 500, max_ms: int = 1500):
    """Add human-like random delay."""
    delay = random.randint(min_ms, max_ms) / 1000
    await asyncio.sleep(delay)


async def analyze_oklahoma():
    """
    Analyze the Oklahoma DOC inmate search page with full workflow.
    """
    
    report = {
        "timestamp": datetime.now().isoformat(),
        "state": "OK",
        "url": BASE_URL,
        "workflow": [],
        "findings": [],
        "form_fields": [],
        "result_structure": {},
        "pagination": {},
        "challenges": [],
        "selectors": {},
    }
    
    print("=" * 60)
    print("OKLAHOMA DOC INMATE SEARCH RECONNAISSANCE (v2)")
    print("=" * 60)
    
    async with async_playwright() as p:
        # Launch visible browser for debugging
        browser = await p.chromium.launch(
            headless=False,
            args=[
                "--disable-blink-features=AutomationControlled",
            ]
        )
        context = await browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            locale="en-US",
            timezone_id="America/Chicago",  # Oklahoma timezone
        )
        
        # Add stealth scripts
        await context.add_init_script("""
            // Hide webdriver
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            
            // Hide automation
            window.chrome = { runtime: {} };
            
            // Realistic plugins
            Object.defineProperty(navigator, 'plugins', {
                get: () => [1, 2, 3, 4, 5]
            });
            
            // Realistic languages
            Object.defineProperty(navigator, 'languages', {
                get: () => ['en-US', 'en']
            });
        """)
        
        page = await context.new_page()
        
        # =====================================================================
        # Step 1: Load homepage
        # =====================================================================
        print("\n[1/8] Loading Oklahoma DOC homepage...")
        try:
            await page.goto(BASE_URL, wait_until="networkidle", timeout=30000)
            print(f"    ✓ Page loaded: {page.url}")
            report["workflow"].append("Homepage loaded")
            
            OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
            await page.screenshot(path=str(OUTPUT_DIR / "ok_recon_01_homepage.png"))
            print("    ✓ Screenshot: ok_recon_01_homepage.png")
            
        except Exception as e:
            print(f"    ✗ Failed: {e}")
            report["challenges"].append(f"Homepage load failed: {str(e)}")
            await browser.close()
            return report
        
        # =====================================================================
        # Step 2: Click Accept button
        # =====================================================================
        print("\n[2/8] Clicking Accept button...")
        try:
            await human_delay(500, 1000)
            
            accept_btn = await page.wait_for_selector(
                "button:has-text('Accept'), input[value='Accept']",
                timeout=5000
            )
            if accept_btn:
                await accept_btn.click()
                print("    ✓ Clicked Accept button")
                report["workflow"].append("Clicked Accept")
                await human_delay(1000, 2000)
            else:
                print("    ⚠ Accept button not found (may already be accepted)")
                
        except Exception as e:
            print(f"    ⚠ Accept button issue: {e}")
            # Continue anyway - might already be accepted
        
        await page.screenshot(path=str(OUTPUT_DIR / "ok_recon_02_after_accept.png"))
        print("    ✓ Screenshot: ok_recon_02_after_accept.png")
        
        # =====================================================================
        # Step 3: Navigate to Search page
        # =====================================================================
        print("\n[3/8] Navigating to Search page...")
        try:
            await human_delay(500, 1000)
            
            # Look for Search link in navigation
            search_link = await page.wait_for_selector(
                "a:has-text('Search'), nav a:has-text('Search')",
                timeout=5000
            )
            if search_link:
                await search_link.click()
                print("    ✓ Clicked Search link")
                report["workflow"].append("Navigated to Search")
                
                await page.wait_for_load_state("networkidle", timeout=10000)
                print(f"    ✓ Search page loaded: {page.url}")
                report["selectors"]["search_page_url"] = page.url
            else:
                print("    ✗ Search link not found")
                report["challenges"].append("Search link not found")
                
        except Exception as e:
            print(f"    ✗ Navigation failed: {e}")
            report["challenges"].append(f"Search navigation failed: {str(e)}")
        
        await human_delay(1000, 2000)
        await page.screenshot(path=str(OUTPUT_DIR / "ok_recon_03_search_page.png"))
        print("    ✓ Screenshot: ok_recon_03_search_page.png")
        
        # =====================================================================
        # Step 4: Analyze search form
        # =====================================================================
        print("\n[4/8] Analyzing search form...")
        
        # Find all input fields
        inputs = await page.query_selector_all("input:visible")
        print(f"    Found {len(inputs)} visible input field(s)")
        
        for inp in inputs:
            inp_type = await inp.get_attribute("type") or "text"
            inp_name = await inp.get_attribute("name") or ""
            inp_id = await inp.get_attribute("id") or ""
            inp_placeholder = await inp.get_attribute("placeholder") or ""
            inp_class = await inp.get_attribute("class") or ""
            
            if inp_type not in ["hidden", "submit", "button"]:
                field_info = {
                    "type": inp_type,
                    "name": inp_name,
                    "id": inp_id,
                    "placeholder": inp_placeholder,
                    "class": inp_class[:50],  # Truncate
                }
                report["form_fields"].append(field_info)
                print(f"    • Input: type={inp_type}, name='{inp_name}', id='{inp_id}', placeholder='{inp_placeholder}'")
                
                # Try to identify the field purpose
                all_attrs = f"{inp_name} {inp_id} {inp_placeholder} {inp_class}".lower()
                if any(x in all_attrs for x in ["last", "lname", "surname"]):
                    report["selectors"]["last_name"] = f"#{inp_id}" if inp_id else f"[name='{inp_name}']"
                    print(f"        → Identified as LAST NAME field")
                elif any(x in all_attrs for x in ["first", "fname", "given"]):
                    report["selectors"]["first_name"] = f"#{inp_id}" if inp_id else f"[name='{inp_name}']"
                    print(f"        → Identified as FIRST NAME field")
                elif any(x in all_attrs for x in ["doc", "inmate", "offender", "number", "id"]):
                    report["selectors"]["inmate_id"] = f"#{inp_id}" if inp_id else f"[name='{inp_name}']"
                    print(f"        → Identified as INMATE ID field")
        
        # Find select dropdowns
        selects = await page.query_selector_all("select:visible")
        for sel in selects:
            sel_name = await sel.get_attribute("name") or ""
            sel_id = await sel.get_attribute("id") or ""
            print(f"    • Select: name='{sel_name}', id='{sel_id}'")
            
            field_info = {
                "type": "select",
                "name": sel_name,
                "id": sel_id,
            }
            report["form_fields"].append(field_info)
        
        # Find submit buttons
        buttons = await page.query_selector_all("button:visible, input[type='submit']:visible")
        for btn in buttons:
            btn_text = ""
            try:
                btn_text = await btn.inner_text()
            except:
                btn_text = await btn.get_attribute("value") or ""
            btn_type = await btn.get_attribute("type") or "button"
            btn_id = await btn.get_attribute("id") or ""
            
            if btn_text.strip():
                print(f"    • Button: '{btn_text.strip()[:30]}', type={btn_type}, id='{btn_id}'")
                
                if any(x in btn_text.lower() for x in ["search", "find", "submit"]):
                    report["selectors"]["submit_button"] = f"#{btn_id}" if btn_id else f"button:has-text('{btn_text.strip()[:20]}')"
                    print(f"        → Identified as SUBMIT button")
        
        # =====================================================================
        # Step 5: Check for reCAPTCHA
        # =====================================================================
        print("\n[5/8] Checking for reCAPTCHA...")
        
        page_content = await page.content()
        
        # Check for different CAPTCHA types
        captcha_info = {
            "recaptcha_v2": "g-recaptcha" in page_content or "recaptcha" in page_content.lower(),
            "recaptcha_v3": "recaptcha/api.js?render=" in page_content,
            "hcaptcha": "hcaptcha" in page_content.lower(),
            "checkbox_visible": False,
        }
        
        # Try to find the reCAPTCHA iframe
        recaptcha_frame = None
        for frame in page.frames:
            if "recaptcha" in frame.url:
                recaptcha_frame = frame
                captcha_info["recaptcha_frame_url"] = frame.url
                print(f"    ✓ Found reCAPTCHA iframe: {frame.url[:80]}...")
                break
        
        # Check for checkbox
        try:
            checkbox = await page.query_selector("iframe[title*='reCAPTCHA']")
            if checkbox:
                captcha_info["checkbox_visible"] = True
                report["selectors"]["recaptcha_iframe"] = "iframe[title*='reCAPTCHA']"
                print("    ✓ Found reCAPTCHA checkbox iframe")
        except:
            pass
        
        if captcha_info["recaptcha_v2"]:
            print("    ⚠ reCAPTCHA v2 detected (checkbox type)")
            report["findings"].append("reCAPTCHA v2 (checkbox) present")
            report["challenges"].append("reCAPTCHA v2 requires solving")
        elif captcha_info["recaptcha_v3"]:
            print("    ✓ reCAPTCHA v3 detected (invisible, score-based)")
            report["findings"].append("reCAPTCHA v3 (invisible) - may auto-pass with stealth")
        elif captcha_info["hcaptcha"]:
            print("    ⚠ hCaptcha detected")
            report["challenges"].append("hCaptcha requires solving service")
        else:
            print("    ✓ No CAPTCHA detected")
            report["findings"].append("No CAPTCHA on search")
        
        report["captcha_info"] = captcha_info
        
        await page.screenshot(path=str(OUTPUT_DIR / "ok_recon_04_form_analysis.png"))
        print("    ✓ Screenshot: ok_recon_04_form_analysis.png")
        
        # =====================================================================
        # Step 6: Attempt test search (if no CAPTCHA or we want to try)
        # =====================================================================
        print("\n[6/8] Attempting test search...")
        
        # Try to find and fill the last name field
        last_name_selector = report["selectors"].get("last_name")
        
        if not last_name_selector:
            # Try common patterns
            possible_selectors = [
                "input[name*='last' i]",
                "input[id*='last' i]",
                "input[placeholder*='Last' i]",
                "#LastName",
                "#lastName",
                "#txtLastName",
            ]
            for sel in possible_selectors:
                try:
                    field = await page.query_selector(sel)
                    if field and await field.is_visible():
                        last_name_selector = sel
                        report["selectors"]["last_name"] = sel
                        print(f"    ✓ Found last name field: {sel}")
                        break
                except:
                    continue
        
        if last_name_selector:
            try:
                await human_delay(500, 1000)
                await page.fill(last_name_selector, "Smith")
                print("    ✓ Entered 'Smith' in last name field")
                
                # If there's a CAPTCHA, we'll need to handle it
                if captcha_info.get("checkbox_visible"):
                    print("    ⚠ reCAPTCHA checkbox present - attempting to click...")
                    
                    # Try to click the reCAPTCHA checkbox
                    try:
                        recaptcha_iframe = await page.wait_for_selector(
                            "iframe[title*='reCAPTCHA']",
                            timeout=5000
                        )
                        if recaptcha_iframe:
                            frame = await recaptcha_iframe.content_frame()
                            if frame:
                                await human_delay(500, 1000)
                                checkbox = await frame.wait_for_selector(
                                    ".recaptcha-checkbox-border, #recaptcha-anchor",
                                    timeout=5000
                                )
                                if checkbox:
                                    await checkbox.click()
                                    print("    ✓ Clicked reCAPTCHA checkbox")
                                    await human_delay(2000, 4000)  # Wait for verification
                                    
                                    await page.screenshot(path=str(OUTPUT_DIR / "ok_recon_05_after_captcha.png"))
                                    print("    ✓ Screenshot: ok_recon_05_after_captcha.png")
                    except Exception as e:
                        print(f"    ⚠ reCAPTCHA click failed: {e}")
                        report["challenges"].append(f"reCAPTCHA click failed: {str(e)}")
                
                # Try to submit
                submit_selector = report["selectors"].get("submit_button")
                if submit_selector:
                    await human_delay(500, 1000)
                    try:
                        await page.click(submit_selector)
                        print("    ✓ Clicked submit button")
                        await page.wait_for_load_state("networkidle", timeout=15000)
                        print("    ✓ Results page loaded")
                        report["workflow"].append("Search submitted")
                    except Exception as e:
                        print(f"    ⚠ Submit failed: {e}")
                else:
                    # Try to find any search button
                    try:
                        await page.click("button:has-text('Search')")
                        print("    ✓ Clicked Search button")
                        await page.wait_for_load_state("networkidle", timeout=15000)
                        report["workflow"].append("Search submitted")
                    except Exception as e:
                        print(f"    ⚠ Could not find/click search button: {e}")
                
            except Exception as e:
                print(f"    ✗ Search attempt failed: {e}")
                report["challenges"].append(f"Search failed: {str(e)}")
        else:
            print("    ✗ Could not find last name field")
            report["challenges"].append("Last name field not found")
        
        await page.screenshot(path=str(OUTPUT_DIR / "ok_recon_06_results.png"))
        print("    ✓ Screenshot: ok_recon_06_results.png")
        
        # =====================================================================
        # Step 7: Analyze results structure
        # =====================================================================
        print("\n[7/8] Analyzing results structure...")
        
        # Look for results table
        tables = await page.query_selector_all("table")
        if tables:
            print(f"    Found {len(tables)} table(s)")
            
            for i, table in enumerate(tables):
                # Get headers
                headers = await table.query_selector_all("th")
                header_texts = []
                for h in headers:
                    text = await h.inner_text()
                    text = text.strip()
                    if text:
                        header_texts.append(text)
                
                if header_texts:
                    print(f"    • Table {i+1} headers: {header_texts}")
                    report["result_structure"]["table_headers"] = header_texts
                
                # Get data rows
                rows = await table.query_selector_all("tbody tr")
                if rows:
                    print(f"    • Table {i+1} has {len(rows)} data row(s)")
                    report["result_structure"]["row_count"] = len(rows)
                    
                    # Sample first row
                    if len(rows) > 0:
                        first_row = rows[0]
                        cells = await first_row.query_selector_all("td")
                        cell_texts = []
                        for c in cells:
                            text = await c.inner_text()
                            cell_texts.append(text.strip()[:40])
                        print(f"    • Sample row: {cell_texts}")
                        report["result_structure"]["sample_row"] = cell_texts
                        
                        # Look for links in cells (often inmate detail links)
                        links = await first_row.query_selector_all("a")
                        for link in links:
                            href = await link.get_attribute("href")
                            text = await link.inner_text()
                            print(f"    • Link found: '{text.strip()[:30]}' -> {href}")
                            report["selectors"]["detail_link_pattern"] = href
        else:
            print("    No tables found - checking for other result formats...")
            
            # Check for card-style results
            cards = await page.query_selector_all("[class*='card'], [class*='result'], [class*='offender']")
            if cards:
                print(f"    Found {len(cards)} card-style result(s)")
                report["result_structure"]["format"] = "cards"
                report["result_structure"]["count"] = len(cards)
        
        # =====================================================================
        # Step 8: Check pagination
        # =====================================================================
        print("\n[8/8] Checking pagination...")
        
        # Look for pagination elements
        pagination_selectors = [
            ".pagination",
            "[class*='pager']",
            "[class*='page-link']",
            "a:has-text('Next')",
            "button:has-text('Next')",
            "[aria-label*='page']",
        ]
        
        for sel in pagination_selectors:
            try:
                elements = await page.query_selector_all(sel)
                if elements:
                    print(f"    ✓ Found pagination: {sel} ({len(elements)} elements)")
                    report["pagination"]["selector"] = sel
                    report["pagination"]["found"] = True
                    
                    # Try to get page numbers
                    for el in elements[:5]:
                        text = await el.inner_text()
                        if text.strip():
                            print(f"        Text: '{text.strip()[:30]}'")
                    break
            except:
                continue
        else:
            print("    ⚠ No pagination found")
            report["pagination"]["found"] = False
        
        # =====================================================================
        # Generate report
        # =====================================================================
        print("\n" + "=" * 60)
        print("RECONNAISSANCE REPORT")
        print("=" * 60)
        
        print(f"\nURL: {BASE_URL}")
        
        print(f"\nWORKFLOW ({len(report['workflow'])} steps):")
        for step in report["workflow"]:
            print(f"  → {step}")
        
        print(f"\nFINDINGS ({len(report['findings'])}):")
        for finding in report["findings"]:
            print(f"  ✓ {finding}")
        
        print(f"\nFORM FIELDS ({len(report['form_fields'])}):")
        for field in report["form_fields"]:
            print(f"  • {field}")
        
        print(f"\nRESULT STRUCTURE:")
        for key, value in report["result_structure"].items():
            print(f"  • {key}: {value}")
        
        print(f"\nPAGINATION:")
        print(f"  • Found: {report['pagination'].get('found', 'unknown')}")
        
        print(f"\nCHALLENGES ({len(report['challenges'])}):")
        if report["challenges"]:
            for challenge in report["challenges"]:
                print(f"  ⚠ {challenge}")
        else:
            print("  ✓ No major challenges detected")
        
        print(f"\nSELECTORS DISCOVERED:")
        for name, selector in report["selectors"].items():
            print(f"  • {name}: {selector}")
        
        # Save report
        report_path = OUTPUT_DIR / "ok_reconnaissance_report.json"
        with open(report_path, "w") as f:
            json.dump(report, f, indent=2, default=str)
        print(f"\n✓ Full report saved to: {report_path}")
        
        # Keep browser open for manual inspection
        print("\n[MANUAL INSPECTION] Browser will stay open for 60 seconds...")
        print("    Use this time to inspect the page manually.")
        print("    Note any additional fields, buttons, or behaviors.")
        await asyncio.sleep(60)
        
        await browser.close()
        print("    ✓ Browser closed")
    
    return report


if __name__ == "__main__":
    asyncio.run(analyze_oklahoma())
