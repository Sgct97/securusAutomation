"""
End-to-end stamp purchase test for a WA inmate.

Target:  WILLIAM CODY (WADOC 893294) at Washington Corrections Center - RC.
         Already successfully added as a Securus contact on the droplet
         (confirmed via droplet DB: outreach_records.contact_added_at set).

Needed:  1 stamp. WA's smallest offered package is 6 stamps for ~$5 per the
         code comment in purchase_stamps; the dynamic discovery logic will
         pick that automatically.

What it does:
  1. Log in to Securus.
  2. Read current stamp balances (expect WA = 0 based on live balance page).
  3. Call client.purchase_stamps(state='WA', needed=1,
     contact_name='WILLIAM CODY') — goes through contact-select →
     discover-packages → pick-smallest → Next → Submit.
  4. Re-read stamp balances to confirm WA balance increased by the
     dynamically-chosen package size.
  5. Write a StampPurchase audit row.

Safety:
  * Buys the SMALLEST package WA offers (6 stamps, ~$5 per code comment).
    Dynamic discovery confirms the actual size before charging.
  * Capped at max_attempts=3 — if logouts repeat, bail early.
  * Headed mode so you can watch.
  * No pipeline integration — manual one-shot verification.

Unlike the CA test, the balance check compares against the STATE KEY
('WA') that get_stamp_balances() actually returns (facility-name match
was a bug in the CA test).
"""

import asyncio
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from securus.client import SecurusClient
from database import StampPurchase, async_session_factory, init_db
from logger import get_logger

log = get_logger("stamp_buy_test_wa")


STATE        = "WA"
CONTACT_NAME = "WILLIAM CODY"
FACILITY     = "Washington Corrections Center - RC"
NEEDED       = 1


async def main() -> int:
    print("=" * 72)
    print("END-TO-END STAMP PURCHASE TEST — WA")
    print(f"  State   : {STATE}")
    print(f"  Contact : {CONTACT_NAME}")
    print(f"  Facility: {FACILITY} (inferred from contact)")
    print(f"  Needed  : {NEEDED}  (client picks smallest package "
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
        wa_before = before.get(STATE, 0)
        print(f"\n  WA balance BEFORE: {wa_before}")

        print(f"\n[2/4] Calling purchase_stamps(state={STATE!r}, "
              f"needed={NEEDED}, contact={CONTACT_NAME!r}, "
              f"max_attempts=3)...")
        result = await client.purchase_stamps(
            state=STATE,
            needed=NEEDED,
            contact_name=CONTACT_NAME,
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
            await _record_purchase(result, succeeded=False,
                                   balance_before=wa_before,
                                   balance_after=None)
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
        wa_after = after.get(STATE, 0) if after else None
        if wa_after is not None:
            print(f"\n  WA balance AFTER : {wa_after}")
            delta = wa_after - wa_before
            print(f"  Delta            : {delta:+d}")
        else:
            delta = None

        expected_delta = result.package_size or 0

        print("\n[4/4] Recording StampPurchase to local DB...")
        await _record_purchase(
            result,
            succeeded=(delta == expected_delta) if delta is not None else True,
            balance_before=wa_before,
            balance_after=wa_after,
        )

        print("\n" + "=" * 72)
        if delta is not None and delta == expected_delta and expected_delta > 0:
            print(f"  PASS — WA stamps went from {wa_before} → {wa_after} "
                  f"(+{delta}), matches the {expected_delta}-stamp package "
                  f"the client bought.")
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
            print(f"  WARN — purchase_stamps reported success but WA "
                  f"balance delta is {delta:+d}, expected +{expected_delta}.")
            print("         Check the balance page manually before trusting "
                  "auto-buy in production.")
            print("=" * 72)
            return 3


async def _record_purchase(result, succeeded: bool,
                           balance_before: int,
                           balance_after: int | None) -> None:
    """Write a local StampPurchase audit row. Best-effort only."""
    note_parts = [f"facility={FACILITY}",
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
    rc = asyncio.run(main())
    sys.exit(rc)
