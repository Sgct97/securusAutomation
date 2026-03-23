"""
Smoke test for SecurusClient — validates login and reads contacts.
Does NOT add contacts or send messages.
"""

import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from securus.client import SecurusClient
from logger import get_logger

log = get_logger("securus.smoke_test")


async def main():
    log.info("=" * 60)
    log.info("SECURUS CLIENT SMOKE TEST")
    log.info("=" * 60)

    async with SecurusClient(headless=False) as client:
        # Test 1: Login
        log.info("TEST 1: Login")
        result = await client.login()
        assert result, "Login failed"
        log.info("Login: PASS")

        # Test 2: List eMessaging contacts
        log.info("TEST 2: List eMessaging contacts")
        contacts = await client.list_emessaging_contacts()
        log.info("eMessaging contacts", count=len(contacts))
        for c in contacts[:5]:
            log.info("  Contact", name=c["name"], id=c["id"], site=c["site"])

        # Test 3: Get compose dropdown contacts
        log.info("TEST 3: Get compose dropdown contacts")
        compose_contacts = await client.get_compose_contacts()
        log.info("Compose dropdown contacts", count=len(compose_contacts))
        for c in compose_contacts[:5]:
            log.info("  Dropdown", name=c["text"], value=c["value"])

        log.info("=" * 60)
        log.info("ALL SMOKE TESTS PASSED")
        log.info("=" * 60)

        # Save results
        results = {
            "emessaging_contacts": contacts,
            "compose_contacts": compose_contacts,
        }
        out = Path(__file__).resolve().parent.parent / "data" / "smoke_test_results.json"
        out.parent.mkdir(parents=True, exist_ok=True)
        with open(out, "w") as f:
            json.dump(results, f, indent=2)
        log.info("Results saved", path=str(out))


if __name__ == "__main__":
    asyncio.run(main())
