"""
End-to-end stamp purchase test for a CA inmate.

Target:  ADAM RUSSELL (CDCR CC8032) at Sierra Conservation Center
Needed:  1 stamp  (CDCR's smallest offered package is 500 for $5, so the
         refactored client will auto-pick that.)

What it does:
  1. Log in to Securus.
  2. Read current stamp balances (logs + screenshot).
  3. Call client.purchase_stamps(state='CA', needed=1,
     contact_name='ADAM RUSSELL') — goes all the way through
     contact-select → discover-packages → pick-smallest →
     Next → Submit.
  4. Re-read stamp balances to confirm Sierra went up by the amount
     the client reports it bought (500 stamps for CDCR).
  5. Write a StampPurchase row to the local data/inmates.db so we have
     an audit trail.

Safety:
  * Buys the SMALLEST package CDCR offers — currently 500 stamps /
    $5.00 per the live Purchase page. No loops, no retries-of-retries
    wrapping this script — if the underlying purchase_stamps() decides
    to retry on a logout, that's intentional.
  * Headed mode on so you can watch every step.
  * No pipeline integration — this is a manual one-shot verification.
"""

import asyncio
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from securus.client import SecurusClient
from database import StampPurchase, async_session_factory, init_db
from logger import get_logger

log = get_logger("stamp_buy_test")


STATE        = "CA"
CONTACT_NAME = "ADAM RUSSELL"
FACILITY     = "Sierra Conservation Center"
# Ask for just 1 stamp; client picks the smallest package that satisfies.
# For CDCR that is 500 stamps ($5) per the live Purchase page.
NEEDED       = 1


async def main() -> int:
    print("=" * 72)
    print("END-TO-END STAMP PURCHASE TEST")
    print(f"  State   : {STATE}")
    print(f"  Contact : {CONTACT_NAME}")
    print(f"  Facility: {FACILITY} (inferred from contact)")
    print(f"  Needed  : {NEEDED}  (client will pick smallest package "
          f"that covers this)")
    print("=" * 72)
    print()

    await init_db()

    async with SecurusClient() as client:
        if not await client.login():
            log.error("Login failed — aborting test.")
            return 1

        # Cool-off: real users don't log in and immediately slam the Purchase
        # flow. Give Securus' session / bot-score a full minute of "idle" time
        # on the post-login dashboard so we blend in a bit better before
        # touching any money-moving page.
        cooloff = 60
        print(f"\n  Cool-off: idling {cooloff}s on the dashboard before "
              "navigating to Purchase (reduces logout risk)...")
        for remaining in range(cooloff, 0, -10):
            print(f"    ...{remaining}s remaining")
            await asyncio.sleep(min(10, remaining))

        print("\n[1/4] Reading stamp balances BEFORE purchase...")
        before = await client.get_stamp_balances()
        if not before:
            print("  (no balances returned)")
        for fac, bal in before.items():
            print(f"    {bal:>4} | {fac}")
        sierra_before = next(
            (b for f, b in before.items() if "sierra" in f.lower()),
            0,
        )
        print(f"\n  Sierra balance BEFORE: {sierra_before}")

        print(f"\n[2/4] Calling purchase_stamps(state={STATE!r}, "
              f"needed={NEEDED}, contact={CONTACT_NAME!r}, "
              f"max_attempts=3)...")
        # Cap retries hard for this test: if the first few attempts all get
        # logged out, bail so we don't hammer Securus and make things worse.
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
                                   balance_before=sierra_before,
                                   balance_after=None)
            return 2

        print("\n[3/4] Reading stamp balances AFTER purchase...")
        # Hard timeout on the post-purchase balance check: if Securus logs us
        # out here (as it did in one run) the inner retry loop can spin
        # forever re-logging-in. 90s is more than enough for a healthy read.
        try:
            after = await asyncio.wait_for(
                client.get_stamp_balances(), timeout=90)
        except asyncio.TimeoutError:
            print("  WARN: balance re-read timed out after 90s. "
                  "Purchase itself succeeded (see confirmation screenshot); "
                  "skipping delta verification.")
            after = {}
        for fac, bal in after.items():
            print(f"    {bal:>4} | {fac}")
        sierra_after = next(
            (b for f, b in after.items() if "sierra" in f.lower()),
            0,
        )
        print(f"\n  Sierra balance AFTER : {sierra_after}")
        delta = sierra_after - sierra_before
        print(f"  Delta                : {delta:+d}")

        expected_delta = result.package_size or 0

        print("\n[4/4] Recording StampPurchase to local DB...")
        await _record_purchase(result,
                               succeeded=(delta == expected_delta),
                               balance_before=sierra_before,
                               balance_after=sierra_after)

        print("\n" + "=" * 72)
        if delta == expected_delta and expected_delta > 0:
            print(f"  PASS — Sierra Conservation Center stamps went from "
                  f"{sierra_before} → {sierra_after} (+{delta}), matches the "
                  f"{expected_delta}-stamp package the client bought.")
            print("=" * 72)
            return 0
        else:
            print(f"  WARN — purchase_stamps reported success but Sierra "
                  f"balance delta is {delta:+d}, expected +{expected_delta}.")
            print("         Check the balance page manually before trusting "
                  "auto-buy in production.")
            print("=" * 72)
            return 3


async def _record_purchase(result, succeeded: bool,
                           balance_before: int,
                           balance_after: int | None) -> None:
    """Write a local StampPurchase audit row. Best-effort only.

    StampPurchase schema: state, package_size, cost_usd, success,
    error_message, purchased_at (no facility/status/quantity columns).
    We fold balance-before/after + facility into error_message when the
    row represents a success-but-delta-mismatch so we don't lose them.
    """
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
