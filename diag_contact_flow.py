"""Diagnostic: for each state, pull 3 real inmates from our DB, try the
Securus ADD CONTACT flow, and dump the facility-dropdown options Securus
returns. Writes a JSON report — no contacts actually added."""

import asyncio
import json
import sys
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).resolve().parent))

from sqlalchemy import select
from playwright.async_api import TimeoutError as PwTimeout

from securus.client import SecurusClient
from database import Inmate, async_session_factory
from logger import get_logger

log = get_logger("diag")

STATES = [
    ("WA", "Washington"),
    ("OK", "Oklahoma"),
    ("NY", "New York"),
    ("CA", "California"),
    ("AR", "Arkansas"),
]


async def sample_inmates(state: str, n: int = 3) -> list[dict]:
    async with async_session_factory() as s:
        q = (select(Inmate)
             .where(Inmate.state == state,
                    Inmate.status == "active",
                    ~Inmate.name.like("UNKNOWN%"))
             .order_by(Inmate.discovered_at.desc())
             .limit(n))
        r = await s.execute(q)
        out = []
        for inm in r.scalars().all():
            parts = inm.name.split(",", 1)
            if len(parts) == 2:
                last = parts[0].strip()
                first = parts[1].strip().split()[0]
            else:
                tokens = inm.name.strip().split()
                first = tokens[0] if tokens else ""
                last = tokens[-1] if len(tokens) > 1 else ""
            out.append({
                "db_name": inm.name,
                "inmate_id": inm.inmate_id,
                "facility": inm.facility,
                "first": first,
                "last": last,
            })
        return out


async def diagnose_state(client: SecurusClient, state_abbr: str,
                         state_full: str, inmate: dict) -> dict:
    """Walk ADD CONTACT up to the search step, collect dropdown data."""
    report = {
        "state": state_abbr,
        "db_name": inmate["db_name"],
        "inmate_id": inmate["inmate_id"],
        "db_facility": inmate["facility"],
        "agency_options": [],
        "search_outcome": None,
        "error": None,
    }

    try:
        await client._ensure_logged_in()
        await client.page.goto(client.EMESSAGE_INBOX_URL,
                               wait_until="domcontentloaded")
        await client.page.wait_for_timeout(3000)
        await client._dismiss_overlays()

        await client.page.locator("text=Contacts").first.click()
        await client.page.wait_for_timeout(2000)
        await client.page.locator("text=ADD CONTACT").first.click()
        await client.page.wait_for_timeout(2000)

        # Use inmate-ID radio + fill
        id_radio = client.page.locator("input[type='radio']").nth(1)
        await id_radio.click()
        await client._human_delay()
        inputs = client.page.locator("input[type='text']:visible")
        await inputs.first.fill(inmate["inmate_id"])

        # Pick the State dropdown
        selects = client.page.locator("select:visible")
        state_select = None
        for i in range(await selects.count()):
            sel = selects.nth(i)
            aria = await sel.get_attribute("aria-label") or ""
            if "navigation" in aria.lower():
                continue
            first_opt = await sel.evaluate("s => s.options[0]?.text || ''")
            if first_opt == "Select":
                state_select = sel
                break

        if not state_select:
            report["error"] = "state dropdown not found"
            return report

        await state_select.select_option(label=state_full)
        await client.page.wait_for_timeout(2000)

        # Find the agency dropdown (the one still on "Select")
        selects = client.page.locator("select:visible")
        agency_select = None
        for i in range(await selects.count()):
            sel = selects.nth(i)
            aria = await sel.get_attribute("aria-label") or ""
            if "navigation" in aria.lower():
                continue
            first_opt = await sel.evaluate("s => s.options[0]?.text || ''")
            if first_opt == "Select":
                selected = await sel.evaluate(
                    "s => s.options[s.selectedIndex]?.text || ''")
                if selected == "Select":
                    agency_select = sel
                    break

        if not agency_select:
            report["error"] = "agency dropdown not found"
            return report

        opts = await agency_select.evaluate(
            "s => Array.from(s.options).map(o => o.text)")
        report["agency_options"] = [o for o in opts if o.strip().lower() != "select"]

        if not report["agency_options"]:
            report["search_outcome"] = "NO_AGENCIES_LISTED"
            return report

        # Pick the first real agency and do the search
        await agency_select.select_option(index=1)
        await client.page.wait_for_timeout(1000)

        await client.page.locator("button:has-text('SEARCH')").first.click()
        await client.page.wait_for_timeout(3000)

        body = await client.page.locator("body").text_content() or ""
        if "CONTACT CANNOT BE FOUND" in body.upper():
            report["search_outcome"] = "CONTACT_NOT_FOUND_POPUP"
            try:
                await client.page.locator(
                    "button:has-text('CLOSE'), a:has-text('CLOSE')"
                ).first.click(timeout=2000)
            except PwTimeout:
                pass
        elif "ADD CONTACT" in body.upper() and inmate["last"].upper() in body.upper():
            report["search_outcome"] = "FOUND"
        else:
            report["search_outcome"] = "OTHER"

    except Exception as e:
        report["error"] = str(e)[:200]

    return report


async def main():
    results = []
    async with SecurusClient(headless=False) as client:
        await client.login()
        log.info("Login OK — starting diagnostics")

        for abbr, full in STATES:
            inmates = await sample_inmates(abbr, n=2)
            log.info(f"=== {abbr} ({len(inmates)} samples) ===")
            for inm in inmates:
                log.info("Diagnosing", state=abbr, inmate=inm["db_name"])
                rep = await diagnose_state(client, abbr, full, inm)
                results.append(rep)
                log.info("Result", **{k: v for k, v in rep.items()
                                      if k != "agency_options"},
                         num_agencies=len(rep["agency_options"]))

    out_path = Path(__file__).resolve().parent / "diag_contact_report.json"
    with out_path.open("w") as f:
        json.dump(results, f, indent=2)
    log.info("Report written", path=str(out_path))

    print("\n" + "=" * 60)
    print("DIAGNOSTIC SUMMARY")
    print("=" * 60)
    for r in results:
        print(f"\n{r['state']} — {r['db_name']} (id={r['inmate_id']})")
        print(f"  our facility: {r['db_facility']!r}")
        print(f"  agencies listed: {len(r['agency_options'])}")
        for a in r['agency_options'][:5]:
            print(f"     - {a}")
        if len(r['agency_options']) > 5:
            print(f"     ... +{len(r['agency_options'])-5} more")
        print(f"  search outcome: {r['search_outcome']}")
        if r['error']:
            print(f"  ERROR: {r['error']}")


if __name__ == "__main__":
    asyncio.run(main())
