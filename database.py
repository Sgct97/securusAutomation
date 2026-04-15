"""
Database models and async session management.
Uses SQLAlchemy 2.0 async patterns.
"""

from datetime import datetime
from typing import Optional, AsyncGenerator
from enum import Enum

from sqlalchemy import (
    String, Integer, Float, DateTime, Boolean, Text, ForeignKey,
    Index, UniqueConstraint, func
)
from sqlalchemy.orm import (
    DeclarativeBase, Mapped, mapped_column, relationship
)
from sqlalchemy.ext.asyncio import (
    AsyncSession, create_async_engine, async_sessionmaker
)

from config import settings


# =============================================================================
# ENUMS
# =============================================================================

class InmateStatus(str, Enum):
    """Status of an inmate record."""
    ACTIVE = "active"           # Currently incarcerated
    RELEASED = "released"       # No longer in custody
    TRANSFERRED = "transferred" # Moved to different facility
    UNKNOWN = "unknown"         # Status not determined


class OutreachStatus(str, Enum):
    """Status of outreach attempt."""
    PENDING = "pending"         # Not yet attempted
    CONTACT_ADDED = "contact_added"  # Added to Securus contacts
    MESSAGE_SENT = "message_sent"    # Message successfully sent
    FAILED = "failed"           # Failed (see error_message)
    RESPONDED = "responded"     # Inmate responded


class ActionType(str, Enum):
    """Types of logged actions."""
    SCRAPE = "scrape"
    ADD_CONTACT = "add_contact"
    SEND_MESSAGE = "send_message"
    LOGIN = "login"
    ERROR = "error"


# =============================================================================
# BASE MODEL
# =============================================================================

class Base(DeclarativeBase):
    """Base class for all models with common columns."""
    pass


# =============================================================================
# MODELS
# =============================================================================

class Inmate(Base):
    """
    Discovered inmate from state databases.
    Each inmate is uniquely identified by (inmate_id, state).
    """
    __tablename__ = "inmates"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    
    # Core identification
    inmate_id: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    state: Mapped[str] = mapped_column(String(2), nullable=False, index=True)
    facility: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    
    # Additional data (if available from scraping)
    release_date: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    status: Mapped[str] = mapped_column(
        String(20), 
        default=InmateStatus.ACTIVE.value,
        index=True
    )
    
    # Admission / book date (from detail pages like VINELink)
    admission_date: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    # Scraping metadata
    source_url: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    discovered_at: Mapped[datetime] = mapped_column(
        DateTime, 
        default=func.now(),
        nullable=False
    )
    last_verified: Mapped[datetime] = mapped_column(
        DateTime,
        default=func.now(),
        onupdate=func.now(),
        nullable=False
    )
    
    # Securus-specific (populated after adding as contact)
    securus_contact_id: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    
    # Relationship to outreach records
    outreach_records: Mapped[list["OutreachRecord"]] = relationship(
        "OutreachRecord",
        back_populates="inmate",
        cascade="all, delete-orphan"
    )
    
    # Unique constraint: same inmate_id can exist in different states
    __table_args__ = (
        UniqueConstraint("inmate_id", "state", name="uq_inmate_id_state"),
        Index("ix_inmate_state_status", "state", "status"),
    )
    
    def __repr__(self) -> str:
        return f"<Inmate {self.inmate_id} ({self.state}): {self.name}>"


class OutreachRecord(Base):
    """
    Record of outreach attempts for each inmate.
    Tracks contact addition and message sending separately.
    """
    __tablename__ = "outreach_records"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    
    # Foreign key to inmate
    inmate_id: Mapped[int] = mapped_column(
        Integer, 
        ForeignKey("inmates.id", ondelete="CASCADE"),
        nullable=False,
        index=True
    )
    
    # Status tracking
    status: Mapped[str] = mapped_column(
        String(20),
        default=OutreachStatus.PENDING.value,
        index=True
    )
    
    # Timestamps
    created_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=func.now(),
        nullable=False
    )
    contact_added_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    message_sent_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    response_received_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    
    # Retry tracking
    retry_count: Mapped[int] = mapped_column(Integer, default=0)
    next_retry_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    
    # Error tracking
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    
    # Message tracking
    message_template_version: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    stamp_cost: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    
    # Relationship back to inmate
    inmate: Mapped["Inmate"] = relationship("Inmate", back_populates="outreach_records")
    
    def __repr__(self) -> str:
        return f"<OutreachRecord inmate_id={self.inmate_id} status={self.status}>"


class ScrapeProgress(Base):
    """
    Tracks scraping progress for resumability.
    Allows restarting from where we left off if interrupted.
    """
    __tablename__ = "scrape_progress"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    
    # Identification
    state: Mapped[str] = mapped_column(String(2), nullable=False, unique=True)
    
    # Progress tracking
    last_letter: Mapped[Optional[str]] = mapped_column(String(2), nullable=True)
    last_page: Mapped[int] = mapped_column(Integer, default=0)
    total_found: Mapped[int] = mapped_column(Integer, default=0)
    
    # Status
    status: Mapped[str] = mapped_column(String(20), default="pending")  # pending, in_progress, completed, failed
    
    # Timestamps
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    last_updated: Mapped[datetime] = mapped_column(
        DateTime,
        default=func.now(),
        onupdate=func.now(),
        nullable=False
    )
    
    # Error info
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    
    def __repr__(self) -> str:
        return f"<ScrapeProgress {self.state}: {self.status} @ {self.last_letter}>"


class ActionLog(Base):
    """
    Audit log of all actions taken by the system.
    Useful for debugging and monitoring.
    """
    __tablename__ = "action_logs"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    
    # Action details
    action_type: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    status: Mapped[str] = mapped_column(String(20), nullable=False)  # success, failure, warning
    
    # Context
    target_id: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)  # e.g., inmate_id
    target_type: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)  # e.g., "inmate", "scraper"
    
    # Details
    details: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    
    # Metadata
    duration_ms: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    timestamp: Mapped[datetime] = mapped_column(
        DateTime,
        default=func.now(),
        nullable=False,
        index=True
    )
    
    def __repr__(self) -> str:
        return f"<ActionLog {self.action_type}: {self.status} @ {self.timestamp}>"


class StampPurchase(Base):
    """Audit trail for automated stamp purchases."""
    __tablename__ = "stamp_purchases"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    state: Mapped[str] = mapped_column(String(2), nullable=False, index=True)
    package_size: Mapped[int] = mapped_column(Integer, nullable=False)
    cost_usd: Mapped[float] = mapped_column(Float, nullable=False)
    success: Mapped[bool] = mapped_column(Boolean, nullable=False)
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    purchased_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=func.now(),
        nullable=False,
        index=True,
    )

    def __repr__(self) -> str:
        return f"<StampPurchase {self.state} {self.package_size}pk {'OK' if self.success else 'FAIL'}>"


# =============================================================================
# DATABASE ENGINE & SESSION
# =============================================================================

# Create async engine
engine = create_async_engine(
    settings.database_url,
    echo=settings.log_level == "DEBUG",
    future=True,
)

# Create async session factory
async_session_factory = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


async def init_db() -> None:
    """Initialize database tables."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    """Dependency for getting async database sessions."""
    async with async_session_factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


# =============================================================================
# CONVENIENCE FUNCTIONS
# =============================================================================

async def get_uncontacted_inmates(
    session: AsyncSession,
    state: Optional[str] = None,
    limit: int = 50
) -> list[Inmate]:
    """
    Get inmates that haven't been contacted yet.
    
    Args:
        session: Database session
        state: Optional state filter
        limit: Maximum number to return
    
    Returns:
        List of Inmate objects without outreach records
    """
    from sqlalchemy import select, not_, exists
    
    # Subquery: inmates with outreach records
    has_outreach = exists(
        select(OutreachRecord.id)
        .where(OutreachRecord.inmate_id == Inmate.id)
    )
    
    query = (
        select(Inmate)
        .where(
            not_(has_outreach),
            Inmate.status == InmateStatus.ACTIVE.value
        )
        .order_by(Inmate.discovered_at.desc())
        .limit(limit)
    )
    
    if state:
        query = query.where(Inmate.state == state)
    
    result = await session.execute(query)
    return list(result.scalars().all())


async def record_action(
    session: AsyncSession,
    action_type: str,
    status: str,
    target_id: Optional[str] = None,
    target_type: Optional[str] = None,
    details: Optional[str] = None,
    error_message: Optional[str] = None,
    duration_ms: Optional[int] = None,
) -> ActionLog:
    """
    Record an action to the audit log.
    
    Args:
        session: Database session
        action_type: Type of action (scrape, add_contact, etc.)
        status: Status (success, failure, warning)
        target_id: Optional target identifier
        target_type: Optional target type
        details: Optional details string
        error_message: Optional error message
        duration_ms: Optional duration in milliseconds
    
    Returns:
        Created ActionLog object
    """
    log = ActionLog(
        action_type=action_type,
        status=status,
        target_id=target_id,
        target_type=target_type,
        details=details,
        error_message=error_message,
        duration_ms=duration_ms,
    )
    session.add(log)
    await session.flush()
    return log

