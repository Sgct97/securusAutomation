"""
Oklahoma DOC Number Collection Script

Collects DOC numbers from search results to analyze the ID pattern.
Searches common last names and extracts DOC# from results.
"""

import asyncio
import json
import random
from datetime import datetime
from pathlib import Path

from playwright.async_api import async_playwright


BASE_URL = "https://okoffender.doc.ok.gov/"
OUTPUT_DIR = Path(__file__).parent.parent / "data"

# Common last names to search
COMMON_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones",
    "Garcia", "Miller", "Davis", "Rodriguez", "Martinez",
    "Wilson", "Anderson", "Taylor", "Thomas", "Moore",
    "Jackson", "Martin", "Lee", "Thompson", "White",
    "Harris", "Clark", "Lewis", "Robinson", "Walker",
]


async def human_delay(min_ms: int = 800, max_ms: int = 2000):
    """Add human-like random delay."""
    delay = random.randint(min_ms, max_ms) / 1000
    await asyncio.sleep(delay)


async def collect_doc_numbers():
    """
    Search common names and collect DOC numbers for pattern analysis.
    """
    
    collected_ids = []
    
    print("=" * 60)
    print("OKLAHOMA DOC NUMBER COLLECTION")
    print("=" * 60)
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=False,  # Visible so you can solve CAPTCHA if needed
            args=["--disable-blink-features=AutomationControlled"]
        )
        
        context = await browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            locale="en-US",
            timezone_id="America/Chicago",
        )
        
        # Stealth
        await context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            window.chrome = { runtime: {} };
        """)
        
        page = await context.new_page()
        
        # =====================================================================
        # Step 1: Navigate and accept terms
        # =====================================================================
        print("\n[1] Loading page and accepting terms...")
        await page.goto(BASE_URL, wait_until="networkidle", timeout=30000)
        await human_delay(1000, 2000)
        
        # Click Accept
        try:
            accept_btn = await page.wait_for_selector("button:has-text('Accept')", timeout=5000)
            if accept_btn:
                await accept_btn.click()
                print("    ✓ Clicked Accept")
                await human_delay(1000, 2000)
        except:
            print("    ⚠ No Accept button (may already be accepted)")
        
        # =====================================================================
        # Step 2: Navigate to Search
        # =====================================================================
        print("\n[2] Navigating to Search page...")
        try:
            search_link = await page.wait_for_selector("a:has-text('Search')", timeout=5000)
            if search_link:
                await search_link.click()
                await page.wait_for_load_state("networkidle", timeout=10000)
                print(f"    ✓ On search page: {page.url}")
        except Exception as e:
            print(f"    ✗ Failed to navigate: {e}")
            await browser.close()
            return []
        
        await human_delay(1000, 2000)
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        await page.screenshot(path=str(OUTPUT_DIR / "ok_collect_01_search.png"))
        
        # =====================================================================
        # Step 3: First search - may need CAPTCHA solving
        # =====================================================================
        print("\n[3] First search (may require CAPTCHA)...")
        print("    ⚠ If image CAPTCHA appears, please solve it manually!")
        print("    ⚠ Browser will wait for you...")
        
        # Find the last name field
        last_name_field = None
        for selector in ["input[id*='last' i]", "input[name*='last' i]", "input[placeholder*='Last' i]"]:
            try:
                field = await page.query_selector(selector)
                if field and await field.is_visible():
                    last_name_field = field
                    print(f"    ✓ Found last name field: {selector}")
                    break
            except:
                continue
        
        if not last_name_field:
            print("    ✗ Could not find last name field")
            print("    Trying to find all visible inputs...")
            inputs = await page.query_selector_all("input:visible")
            for i, inp in enumerate(inputs):
                inp_id = await inp.get_attribute("id") or ""
                inp_name = await inp.get_attribute("name") or ""
                inp_placeholder = await inp.get_attribute("placeholder") or ""
                print(f"    • Input {i}: id='{inp_id}', name='{inp_name}', placeholder='{inp_placeholder}'")
            await browser.close()
            return []
        
        # =====================================================================
        # Step 4: Search each common name
        # =====================================================================
        for i, name in enumerate(COMMON_NAMES):
            print(f"\n[4.{i+1}] Searching for '{name}'...")
            
            try:
                # Clear and fill
                await last_name_field.fill("")
                await human_delay(200, 400)
                await last_name_field.fill(name)
                await human_delay(500, 1000)
                
                # Handle CAPTCHA if present
                try:
                    recaptcha_iframe = await page.query_selector("iframe[title*='reCAPTCHA']")
                    if recaptcha_iframe:
                        print("    ⚠ reCAPTCHA detected - attempting to click checkbox...")
                        frame = await recaptcha_iframe.content_frame()
                        if frame:
                            checkbox = await frame.query_selector(".recaptcha-checkbox-border")
                            if checkbox:
                                await human_delay(500, 1000)
                                await checkbox.click()
                                print("    ⚠ Clicked checkbox - waiting for challenge or pass...")
                                await human_delay(3000, 5000)
                                
                                # Check if image challenge appeared
                                # If so, wait for user to solve
                                await page.screenshot(path=str(OUTPUT_DIR / f"ok_collect_captcha_{name}.png"))
                except Exception as e:
                    print(f"    ⚠ CAPTCHA handling: {e}")
                
                # Click Search button
                search_btn = await page.query_selector("button:has-text('Search')")
                if search_btn:
                    await search_btn.click()
                    print("    ✓ Clicked Search")
                else:
                    # Try submit
                    await page.keyboard.press("Enter")
                    print("    ✓ Pressed Enter to submit")
                
                # Wait for results
                await page.wait_for_load_state("networkidle", timeout=30000)
                await human_delay(1000, 2000)
                
                # Extract DOC numbers from results
                # Look for table rows or result cards
                doc_numbers = []
                
                # Try to find DOC# in table cells or text
                # Common patterns: "DOC#: 12345", table with DOC column, etc.
                
                # Method 1: Look for table with headers
                tables = await page.query_selector_all("table")
                for table in tables:
                    rows = await table.query_selector_all("tr")
                    for row in rows:
                        cells = await row.query_selector_all("td")
                        for cell in cells:
                            text = await cell.inner_text()
                            text = text.strip()
                            # DOC numbers might be like "12345678" or "2025xxxxx"
                            if text and text.replace("-", "").isdigit() and len(text) >= 6:
                                doc_numbers.append(text)
                            # Or might have "DOC#:" prefix
                            if "DOC" in text.upper():
                                # Extract number after DOC
                                import re
                                matches = re.findall(r'\d{6,}', text)
                                doc_numbers.extend(matches)
                
                # Method 2: Look for links that might contain IDs
                links = await page.query_selector_all("a")
                for link in links:
                    href = await link.get_attribute("href") or ""
                    text = await link.inner_text()
                    # Check if href or text contains a DOC number pattern
                    import re
                    matches = re.findall(r'\d{6,}', href + " " + text)
                    doc_numbers.extend(matches)
                
                # Method 3: Search entire page text for number patterns
                page_text = await page.inner_text("body")
                import re
                # Look for 7-9 digit numbers (likely DOC#s)
                all_numbers = re.findall(r'\b\d{7,9}\b', page_text)
                doc_numbers.extend(all_numbers)
                
                # Also look for 2025-prefixed numbers
                year_prefixed = re.findall(r'\b2025\d{3,6}\b', page_text)
                doc_numbers.extend(year_prefixed)
                
                # Deduplicate
                doc_numbers = list(set(doc_numbers))
                
                if doc_numbers:
                    print(f"    ✓ Found {len(doc_numbers)} potential DOC#s: {doc_numbers[:5]}...")
                    for doc in doc_numbers:
                        collected_ids.append({
                            "doc_number": doc,
                            "search_name": name,
                            "timestamp": datetime.now().isoformat()
                        })
                else:
                    print("    ⚠ No DOC numbers found in results")
                    # Take screenshot for debugging
                    await page.screenshot(path=str(OUTPUT_DIR / f"ok_collect_result_{name}.png"))
                
                await human_delay(2000, 4000)
                
            except Exception as e:
                print(f"    ✗ Error searching '{name}': {e}")
                await page.screenshot(path=str(OUTPUT_DIR / f"ok_collect_error_{name}.png"))
                continue
        
        # =====================================================================
        # Save results
        # =====================================================================
        print("\n" + "=" * 60)
        print("COLLECTION COMPLETE")
        print("=" * 60)
        
        # Deduplicate by doc_number
        unique_ids = {}
        for item in collected_ids:
            unique_ids[item["doc_number"]] = item
        
        collected_ids = list(unique_ids.values())
        
        print(f"\nTotal unique DOC#s collected: {len(collected_ids)}")
        
        if collected_ids:
            # Save to file
            output_file = OUTPUT_DIR / "ok_doc_numbers.json"
            with open(output_file, "w") as f:
                json.dump(collected_ids, f, indent=2)
            print(f"✓ Saved to: {output_file}")
            
            # Print sample for analysis
            print("\nSample DOC#s for pattern analysis:")
            for item in collected_ids[:20]:
                print(f"  • {item['doc_number']} (from search: {item['search_name']})")
            
            # Quick pattern analysis
            print("\nQuick pattern analysis:")
            by_prefix = {}
            for item in collected_ids:
                doc = item["doc_number"]
                if len(doc) >= 4:
                    prefix = doc[:4]
                    by_prefix[prefix] = by_prefix.get(prefix, 0) + 1
            
            for prefix, count in sorted(by_prefix.items()):
                print(f"  Prefix '{prefix}': {count} occurrences")
        
        print("\n[MANUAL REVIEW] Browser staying open for 30 seconds...")
        await asyncio.sleep(30)
        
        await browser.close()
    
    return collected_ids


if __name__ == "__main__":
    results = asyncio.run(collect_doc_numbers())
    print(f"\nDone. Collected {len(results)} DOC numbers.")


