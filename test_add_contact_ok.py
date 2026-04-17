"""
Live one-shot test: add an OK DOC inmate as a contact on Securus.

Target: DOUGLAS ALAN SIMPSON (OK DOC #0000134726) at Jackie Brannon
        Correctional Center. User manually confirmed this inmate IS
        findable on Securus via first+last name search in the OK DOC
        agency, and that stripping the leading zeros from the ID allows
        an ID search to find him too.

Purpose: verify the two add_contact fixes we just made actually succeed
         end-to-end on the live site:
  1) OK IDs get zero-stripped before submission (0000134726 -> 134726).
  2) STATE_TO_AGENCY_HINT falls back to "Oklahoma Department of
     Corrections" when our stored facility name ("JACKIE BRANNON
     CORRECTIONAL CENTER") doesn't match either OK agency option in the
     Securus dropdown.

This will CREATE a real contact on the client's Securus account.
Contact creation itself costs nothing; it's the same side effect the
production pipeline would have if the fix works.
"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from securus.client import SecurusClient
from logger import get_logger

log = get_logger("add_contact_test_ok")


FIRST_NAME = "Douglas"
LAST_NAME  = "Simpson"
INMATE_ID  = "0000134726"
STATE      = "OK"
FACILITY   = "JACKIE BRANNON CORRECTIONAL CENTER"


async def main() -> int:
    print("=" * 72)
    print("LIVE add_contact TEST — OK")
    print(f"  Name      : {FIRST_NAME} {LAST_NAME}")
    print(f"  DOC ID    : {INMATE_ID}  (expect code to strip to "
          f"{INMATE_ID.lstrip('0')!r})")
    print(f"  State     : {STATE}")
    print(f"  Facility  : {FACILITY}  (expect fallback to "
          f"'Oklahoma Department of Corrections' agency)")
    print("=" * 72)
    print()

    async with SecurusClient() as client:
        if not await client.login():
            print("Login failed.")
            return 1

        cooloff = 60
        print(f"\n  Cool-off: idling {cooloff}s post-login...")
        await asyncio.sleep(cooloff)

        print("\n  Calling add_contact...\n")
        result = await client.add_contact(
            first_name=FIRST_NAME,
            last_name=LAST_NAME,
            state=STATE,
            facility=FACILITY,
            inmate_id=INMATE_ID,
        )

        print("\n" + "=" * 72)
        print("RESULT")
        print(f"  success    = {result.success}")
        print(f"  inmate_id  = {result.inmate_id}")
        print(f"  name       = {result.name}")
        print(f"  state      = {result.state}")
        print(f"  facility   = {result.facility}")
        if result.error:
            print(f"  error      = {result.error}")
        if result.screenshot_path:
            print(f"  screenshot = {result.screenshot_path}")
        print("=" * 72)

        return 0 if result.success else 2


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
