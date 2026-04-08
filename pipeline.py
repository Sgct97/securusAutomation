"""
Daily outreach pipeline — the single entry point for automated operation.

Run via cron: 0 9 * * * cd /opt/securusAutomation && venv/bin/python pipeline.py

What it does each run:
  1. Checks if scraping is due (based on SCRAPE_INTERVAL_DAYS)
  2. If due, runs all configured scrapers and creates outreach records
  3. Pulls pending outreach candidates (up to DAILY_MESSAGE_LIMIT)
  4. Logs into Securus, sends messages, updates DB
  5. Logs a summary
"""

import asyncio
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

import structlog
from sqlalchemy import select, func, update, or_

sys.path.insert(0, str(Path(__file__).resolve().parent))

from config import settings
from database import (
    Inmate, OutreachRecord, OutreachStatus, ScrapeProgress,
    async_session_factory, engine, Base,
)
from securus.client import SecurusClient
from securus.message_template import SUBJECT, BODY

log = structlog.get_logger()

STATE_ABBR_TO_FULL = {
    "WA": "Washington",
    "OK": "Oklahoma",
    "NY": "New York",
    "CA": "California",
    "AR": "Arkansas",
}

STATE_TO_AGENCY = {
    "WA": "Washington State Department of Corrections",
    "OK": "Oklahoma Department of Corrections",
    "NY": "NYS DOCCS Inmate Services",
    "CA": "California Department of Corrections & Rehabilitation",
    "AR": "Arkansas DOC",
}

MAX_RETRIES = 3


# =========================================================================
# STEP 1: SCRAPING
# =========================================================================

async def should_scrape() -> bool:
    """Check if enough time has passed since the last scrape."""
    async with async_session_factory() as session:
        result = await session.execute(
            select(func.max(ScrapeProgress.completed_at))
        )
        last_scrape = result.scalar_one_or_none()

    if last_scrape is None:
        return True

    cutoff = datetime.now(timezone.utc) - timedelta(days=settings.scrape_interval_days)
    if last_scrape.tzinfo is None:
        last_scrape = last_scrape.replace(tzinfo=timezone.utc)
    return last_scrape < cutoff


async def run_scraper(state: str) -> int:
    """Run a single state's scraper. Returns count of new inmates found."""
    log.info("Running scraper", state=state)
    started = datetime.now(timezone.utc)
    new_count = 0

    try:
        if state == "WA":
            from scrapers.washington_scraper import run
            await run(max_pages=20)
        elif state == "OK":
            from scrapers.oklahoma_parser import run
            await run(days_back=30)
        elif state == "NY":
            from scrapers.newyork_scraper import run
            await run(max_count=200, stop_after_misses=15)
        elif state == "CA":
            from scrapers.california_scraper import run
            await run(max_count=200, stop_after_misses=15)
        elif state == "AR":
            from scrapers.arkansas_scraper import run
            await run(max_misses=50)
        else:
            log.warning("Unknown state, skipping", state=state)
            return 0

        # Count how many were discovered since we started
        async with async_session_factory() as session:
            result = await session.execute(
                select(func.count()).select_from(Inmate).where(
                    Inmate.state == state,
                    Inmate.discovered_at >= started,
                )
            )
            new_count = result.scalar() or 0

        # Update scrape progress
        async with async_session_factory() as session:
            progress = (await session.execute(
                select(ScrapeProgress).where(ScrapeProgress.state == state)
            )).scalar_one_or_none()

            if progress:
                progress.status = "completed"
                progress.completed_at = datetime.now(timezone.utc)
                progress.total_found = (progress.total_found or 0) + new_count
            else:
                session.add(ScrapeProgress(
                    state=state,
                    status="completed",
                    started_at=started,
                    completed_at=datetime.now(timezone.utc),
                    total_found=new_count,
                ))
            await session.commit()

        log.info("Scraper complete", state=state, new_inmates=new_count)

    except Exception as e:
        log.error("Scraper failed", state=state, error=str(e))

    return new_count


async def run_all_scrapers():
    """Run scrapers for all configured states."""
    states = [s.strip() for s in settings.states_to_scrape.split(",") if s.strip()]
    log.info("Starting scrapers", states=states)

    total_new = 0
    for state in states:
        new = await run_scraper(state)
        total_new += new

    log.info("All scrapers complete", total_new_inmates=total_new)
    return total_new


# =========================================================================
# STEP 2: CREATE OUTREACH RECORDS FOR NEW INMATES
# =========================================================================

async def create_outreach_for_new_inmates() -> int:
    """Create pending OutreachRecords for inmates that don't have one yet."""
    async with async_session_factory() as session:
        result = await session.execute(
            select(Inmate).where(
                ~Inmate.id.in_(
                    select(OutreachRecord.inmate_id)
                ),
                Inmate.status == "active",
            ).order_by(Inmate.discovered_at.desc())
        )
        inmates_without_outreach = result.scalars().all()

        created = 0
        for inmate in inmates_without_outreach:
            session.add(OutreachRecord(
                inmate_id=inmate.id,
                status=OutreachStatus.PENDING.value,
                created_at=datetime.now(timezone.utc),
            ))
            created += 1

        if created > 0:
            await session.commit()

    log.info("Outreach records created", count=created)
    return created


# =========================================================================
# STEP 3: GET PENDING CANDIDATES (distributed across states)
# =========================================================================

CANDIDATE_POOL_MULTIPLIER = 10

async def get_pending_candidates(limit: int) -> list[dict]:
    """
    Pull a large pool of pending outreach candidates, distributed evenly
    across states. The pool is CANDIDATE_POOL_MULTIPLIER * limit so the
    send loop has plenty of fallbacks when individual inmates fail.
    """
    states = [s.strip() for s in settings.states_to_scrape.split(",") if s.strip()]
    pool_size = limit * CANDIDATE_POOL_MULTIPLIER
    per_state = max(1, pool_size // len(states))
    remainder = pool_size - (per_state * len(states))

    candidates = []

    async with async_session_factory() as session:
        for i, state in enumerate(states):
            state_limit = per_state + (1 if i < remainder else 0)

            result = await session.execute(
                select(OutreachRecord, Inmate)
                .join(Inmate)
                .where(
                    Inmate.state == state,
                    OutreachRecord.status.in_([
                        OutreachStatus.PENDING.value,
                        OutreachStatus.CONTACT_ADDED.value,
                    ]),
                    OutreachRecord.retry_count < MAX_RETRIES,
                    ~Inmate.name.like("UNKNOWN%"),
                    or_(
                        OutreachRecord.next_retry_at == None,
                        OutreachRecord.next_retry_at <= datetime.now(timezone.utc),
                    ),
                )
                .order_by(Inmate.discovered_at.desc())
                .limit(state_limit)
            )

            for record, inmate in result.all():
                name_parts = inmate.name.split(",", 1)
                if len(name_parts) == 2:
                    last_name = name_parts[0].strip()
                    first_name = name_parts[1].strip().split()[0]
                else:
                    parts = inmate.name.strip().split()
                    first_name = parts[0] if parts else ""
                    last_name = parts[-1] if len(parts) > 1 else ""

                candidates.append({
                    "outreach_id": record.id,
                    "outreach_status": record.status,
                    "inmate_db_id": inmate.id,
                    "inmate_id": inmate.inmate_id,
                    "name": inmate.name,
                    "first_name": first_name,
                    "last_name": last_name,
                    "state": inmate.state,
                    "state_full": STATE_ABBR_TO_FULL.get(inmate.state, inmate.state),
                    "facility": inmate.facility or "",
                    "agency": STATE_TO_AGENCY.get(inmate.state, ""),
                })

    log.info("Candidate pool loaded",
             pool_size=len(candidates), send_target=limit,
             by_state={s: sum(1 for c in candidates if c["state"] == s) for s in states})
    return candidates


# =========================================================================
# STEP 4: SEND MESSAGES
# =========================================================================

async def send_outreach(candidates: list[dict], send_target: int) -> dict:
    """
    Log into Securus once, iterate through the candidate pool, and keep
    trying until we've successfully sent `send_target` messages or
    exhausted all candidates. Individual failures (contact not found,
    agency mismatch, etc.) just skip to the next candidate.

    Only stops early for:
      - Out of stamps (can't send anything else)
      - Login failure
    """
    stats = {"sent": 0, "failed": 0, "skipped": 0, "contact_errors": 0}

    if not candidates:
        log.info("No candidates to process")
        return stats

    async with SecurusClient(headless=settings.headless) as client:
        client._last_action_time = 0

        log.info("Logging into Securus")
        try:
            await client.login()
        except Exception as e:
            log.error("Securus login failed, aborting run", error=str(e))
            return stats

        log.info("Login successful",
                 pool_size=len(candidates), send_target=send_target)

        for i, candidate in enumerate(candidates, 1):
            if stats["sent"] >= send_target:
                log.info("Daily send target reached",
                         sent=stats["sent"], target=send_target)
                break

            log.info(f"Processing {i}/{len(candidates)} "
                     f"(sent {stats['sent']}/{send_target})",
                     name=candidate["name"],
                     state=candidate["state"],
                     inmate_id=candidate["inmate_id"])

            try:
                # Re-check record status from DB (guards against duplicate sends
                # if pipeline ran twice or DB was copied mid-run)
                async with async_session_factory() as session:
                    fresh = (await session.execute(
                        select(OutreachRecord).where(
                            OutreachRecord.id == candidate["outreach_id"]
                        )
                    )).scalar_one_or_none()
                    if fresh and fresh.status == OutreachStatus.MESSAGE_SENT.value:
                        log.info("Already sent (detected on re-check), skipping",
                                 name=candidate["name"])
                        stats["skipped"] += 1
                        continue

                # Add contact (skip if already added on a prior run)
                if candidate["outreach_status"] == OutreachStatus.CONTACT_ADDED.value:
                    log.info("Contact already added (prior run), skipping to message",
                             name=candidate["name"])
                else:
                    contact_result = await client.add_contact(
                        first_name=candidate["first_name"],
                        last_name=candidate["last_name"],
                        state=candidate["state_full"],
                        facility=candidate["agency"],
                        inmate_id=candidate["inmate_id"],
                    )

                    if not contact_result.success:
                        err = contact_result.error or "Unknown error"
                        if "already" not in err.lower():
                            log.warning("Failed to add contact, trying next",
                                        name=candidate["name"], error=err)
                            await _mark_failed(candidate["outreach_id"], f"add_contact: {err}")
                            stats["contact_errors"] += 1
                            continue
                        log.info("Contact already exists, proceeding to message",
                                 name=candidate["name"])
                    else:
                        await _mark_contact_added(candidate["outreach_id"])
                        log.info("Contact added", name=candidate["name"])

                # Send message
                contact_name = f"{candidate['first_name']} {candidate['last_name']}".upper()
                msg_result = await client.send_message(
                    contact_name=contact_name,
                    subject=SUBJECT,
                    body=BODY,
                )

                if msg_result.success:
                    await _mark_sent(candidate["outreach_id"])
                    stats["sent"] += 1
                    log.info("Message sent",
                             name=candidate["name"],
                             sent=stats["sent"],
                             target=send_target)
                else:
                    err = msg_result.error or "Unknown error"
                    if "stamp" in err.lower() and ("0" in err or "no" in err.lower()):
                        log.warning("Out of stamps, stopping run", error=err)
                        await _mark_failed(candidate["outreach_id"], f"send: {err}")
                        stats["failed"] += 1
                        break
                    await _mark_failed(candidate["outreach_id"], f"send: {err}")
                    stats["failed"] += 1
                    log.warning("Failed to send, trying next",
                                name=candidate["name"], error=err)

            except Exception as e:
                log.error("Unexpected error, trying next candidate",
                          name=candidate["name"], error=str(e))
                await _mark_failed(candidate["outreach_id"], str(e))
                stats["failed"] += 1

    return stats


# =========================================================================
# DB UPDATE HELPERS
# =========================================================================

async def _mark_contact_added(outreach_id: int):
    async with async_session_factory() as session:
        await session.execute(
            update(OutreachRecord)
            .where(OutreachRecord.id == outreach_id)
            .values(
                status=OutreachStatus.CONTACT_ADDED.value,
                contact_added_at=datetime.now(timezone.utc),
            )
        )
        await session.commit()


async def _mark_sent(outreach_id: int):
    async with async_session_factory() as session:
        await session.execute(
            update(OutreachRecord)
            .where(OutreachRecord.id == outreach_id)
            .values(
                status=OutreachStatus.MESSAGE_SENT.value,
                message_sent_at=datetime.now(timezone.utc),
                stamp_cost=1,
            )
        )
        await session.commit()


async def _mark_failed(outreach_id: int, error: str):
    async with async_session_factory() as session:
        record = (await session.execute(
            select(OutreachRecord).where(OutreachRecord.id == outreach_id)
        )).scalar_one()

        record.retry_count += 1
        record.error_message = error

        if record.retry_count >= MAX_RETRIES:
            record.status = OutreachStatus.FAILED.value
            log.warning("Max retries exceeded, marking as failed",
                        outreach_id=outreach_id)
        else:
            record.status = OutreachStatus.PENDING.value
            record.next_retry_at = datetime.now(timezone.utc) + timedelta(hours=6)

        await session.commit()


# =========================================================================
# MAIN PIPELINE
# =========================================================================

async def run_pipeline():
    """Full pipeline: scrape (if due) → create outreach → send messages."""
    run_start = datetime.now(timezone.utc)
    log.info("=" * 60)
    log.info("Pipeline started", time=run_start.isoformat())
    log.info("=" * 60)

    # Ensure DB tables exist
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # Check daily limit
    if settings.daily_message_limit == 0:
        log.info("DAILY_MESSAGE_LIMIT is 0 — pipeline paused")
        return

    # Step 1: Scrape if due
    if await should_scrape():
        log.info("Scraping is due, running scrapers")
        await run_all_scrapers()
    else:
        log.info("Scraping not due yet, skipping")

    # Step 2: Create outreach records for any new inmates
    await create_outreach_for_new_inmates()

    # Step 3: Get pending candidates
    candidates = await get_pending_candidates(settings.daily_message_limit)

    if not candidates:
        log.info("No pending candidates, nothing to send")
        log.info("Pipeline complete", elapsed=str(datetime.now(timezone.utc) - run_start))
        return

    # Step 4: Send messages (keep trying until we hit the daily limit)
    stats = await send_outreach(candidates, send_target=settings.daily_message_limit)

    # Summary
    elapsed = datetime.now(timezone.utc) - run_start
    log.info("=" * 60)
    log.info("Pipeline complete",
             sent=stats["sent"],
             failed=stats["failed"],
             contact_errors=stats["contact_errors"],
             elapsed=str(elapsed))
    log.info("=" * 60)


if __name__ == "__main__":
    asyncio.run(run_pipeline())
