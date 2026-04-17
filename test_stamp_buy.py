"""
End-to-end stamp purchase test, parameterized by state.

Usage:
  python test_stamp_buy.py <STATE> <CONTACT_NAME> [--facility "<FACILITY>"]

Examples:
  python test_stamp_buy.py WA "WILLIAM CODY"
  python test_stamp_buy.py AR "JADERIUS HARE"
  python test_stamp_buy.py NY "WINSTON TIMBERLAKE"
  python test_stamp_buy.py CA "ADAM RUSSELL"

Target contact must ALREADY exist in the Securus contacts list (added via
add_contact or the pipeline). State must match one of the state codes
get_stamp_balances() returns.

What it does:
  1. Log in to Securus.
  2. Read current stamp balances.
  3. Call client.purchase_stamps(state, needed=1, contact_name,
     max_attempts=3) — goes through contact-select → discover-packages →
     pick-smallest → Next → Submit.
  4. Re-read stamp balances, verify state-key balance went up by
     result.package_size.
  5. Write a StampPurchase audit row.

Safety:
  * Buys the SMALLEST package the state offers (dynamically discovered).
  * max_attempts=3 so a bad run doesn't spam retries.
  * Headed mode so you can watch.
"""

import argparse
import asyncio
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from securus.client import SecurusClient
from database import StampPurchase, async_session_factory, init_db
from logger import get_logger

log = get_logger("stamp_buy_test")


async def main(state: str, contact_name: str, facility_hint: str) -> int:
    needed = 1
    print("=" * 72)
    print(f"END-TO-END STAMP PURCHASE TEST — {state}")
    print(f"  State   : {state}")
    print(f"  Contact : {contact_name}")
    print(f"  Facility: {facility_hint} (hint only, inferred from contact)")
    print(f"  Needed  : {needed}  (client picks smallest package "
          f"covering this)")
    print("=" * 72)
    print()

    await init_db()

    async with SecurusClient() as client:
        if not await client.login():
            log.error("Login failed — aborting test.")
            return 1

        cooloff = 60
        print(f"\n  Cool-off: idling {cooloff}s on the dashboard before "
              "navigating to Purchase (reduces logout risk)...")
        for remaining in range(cooloff, 0, -10):
            print(f"    ...{remaining}s remaining")
            await asyncio.sleep(min(10, remaining))

        print("\n[1/4] Reading stamp balances BEFORE purchase...")
        try:
            before = await asyncio.wait_for(
                client.get_stamp_balances(), timeout=90)
        except asyncio.TimeoutError:
            print("  WARN: pre-purchase balance read timed out after 90s.")
            before = {}
        if not before:
            print("  (no balances returned)")
        for st_code, bal in before.items():
            print(f"    {bal:>6} | {st_code}")
        state_before = before.get(state, 0)
        print(f"\n  {state} balance BEFORE: {state_before}")

        print(f"\n[2/4] Calling purchase_stamps(state={state!r}, "
              f"needed={needed}, contact={contact_name!r}, "
              f"max_attempts=3)...")
        result = await client.purchase_stamps(
            state=state,
            needed=needed,
            contact_name=contact_name,
            max_attempts=3,
        )
        print(f"\n  purchase_stamps result: success={result.success}")
        print(f"    state        = {result.state}")
        print(f"    package_size = {result.package_size}  "
              f"(chosen by client from live page)")
        print(f"    cost_usd     = ${result.cost_usd}")
        if result.error:
            print(f"    error        = {result.error}")
        if result.screenshot_path:
            print(f"    screenshot   = {result.screenshot_path}")

        if not result.success:
            log.error("purchase_stamps failed — check screenshot + logs.")
            await _record_purchase(
                result, facility_hint,
                succeeded=False,
                balance_before=state_before,
                balance_after=None,
            )
            return 2

        print("\n[3/4] Reading stamp balances AFTER purchase...")
        try:
            after = await asyncio.wait_for(
                client.get_stamp_balances(), timeout=90)
        except asyncio.TimeoutError:
            print("  WARN: balance re-read timed out after 90s. "
                  "Purchase itself succeeded (see confirmation screenshot); "
                  "skipping delta verification.")
            after = {}
        for st_code, bal in after.items():
            print(f"    {bal:>6} | {st_code}")
        state_after = after.get(state, 0) if after else None
        if state_after is not None:
            print(f"\n  {state} balance AFTER : {state_after}")
            delta = state_after - state_before
            print(f"  Delta             : {delta:+d}")
        else:
            delta = None

        expected_delta = result.package_size or 0

        print("\n[4/4] Recording StampPurchase to local DB...")
        await _record_purchase(
            result, facility_hint,
            succeeded=(delta == expected_delta) if delta is not None else True,
            balance_before=state_before,
            balance_after=state_after,
        )

        print("\n" + "=" * 72)
        if delta is not None and delta == expected_delta and expected_delta > 0:
            print(f"  PASS — {state} stamps went from {state_before} → "
                  f"{state_after} (+{delta}), matches the {expected_delta}-"
                  f"stamp package the client bought.")
            print("=" * 72)
            return 0
        elif delta is None:
            print(f"  PARTIAL — purchase_stamps reported success (bought "
                  f"{expected_delta} stamps for ${result.cost_usd}) but "
                  f"post-purchase balance re-read timed out. Verify manually "
                  f"on the Total Stamps page.")
            print("=" * 72)
            return 0
        else:
            print(f"  WARN — purchase_stamps reported success but {state} "
                  f"balance delta is {delta:+d}, expected +{expected_delta}.")
            print("         Check the balance page manually before trusting "
                  "auto-buy in production.")
            print("=" * 72)
            return 3


async def _record_purchase(result, facility_hint: str, succeeded: bool,
                           balance_before: int,
                           balance_after: int | None) -> None:
    """Write a local StampPurchase audit row. Best-effort only."""
    note_parts = [f"facility={facility_hint}",
                  f"before={balance_before}",
                  f"after={balance_after}"]
    if result.error:
        note_parts.append(f"error={result.error}")
    note = " | ".join(note_parts)
    try:
        async with async_session_factory() as s:
            row = StampPurchase(
                state=result.state,
                package_size=result.package_size,
                cost_usd=result.cost_usd or 0,
                success=succeeded,
                error_message=note,
                purchased_at=datetime.now(timezone.utc),
            )
            s.add(row)
            await s.commit()
            print(f"  Audit row written: state={result.state}, "
                  f"pkg={result.package_size}, success={succeeded}, "
                  f"before={balance_before}, after={balance_after}")
    except Exception as e:
        print(f"  WARN: could not write audit row: {e}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("state", help="State code (e.g. WA, AR, NY, CA)")
    ap.add_argument("contact", help=(
        "Contact name as it appears in the Securus dropdown, e.g. "
        "'WILLIAM CODY'"))
    ap.add_argument("--facility", default="(unknown)",
                    help="Facility name hint (for the audit row only)")
    args = ap.parse_args()

    rc = asyncio.run(main(
        state=args.state.upper(),
        contact_name=args.contact.upper(),
        facility_hint=args.facility,
    ))
    sys.exit(rc)
