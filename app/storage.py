from datetime import datetime, timezone
from typing import Any

from sqlalchemy import BigInteger, DateTime, Float, Integer, String
from sqlalchemy.dialects.postgresql import JSONB, insert
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class RequestEvent(Base):
    __tablename__ = "request_events"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    event_id: Mapped[str] = mapped_column(String(64), unique=True)
    request_id: Mapped[str] = mapped_column(String(128), index=True)
    trace_id: Mapped[str] = mapped_column(String(32), default="")
    provider: Mapped[str] = mapped_column(String(1))
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    duration_ms: Mapped[float] = mapped_column(Float)
    gateway_status: Mapped[int] = mapped_column(Integer)
    upstream_status: Mapped[int | None] = mapped_column(Integer, nullable=True)
    request_payload: Mapped[dict[str, Any]] = mapped_column(JSONB)
    response_body: Mapped[Any] = mapped_column(JSONB, nullable=True)
    error: Mapped[str | None] = mapped_column(String(256), nullable=True)


async def persist_request_events(
    engine: AsyncEngine,
    events: list[dict[str, Any]],
) -> None:
    async with engine.begin() as connection:
        statement = insert(RequestEvent).on_conflict_do_nothing(
            index_elements=["event_id"]
        )
        await connection.execute(statement, events)


def make_request_event(
    *,
    request_id: str,
    trace_id: str,
    event_id: str,
    provider: str,
    started_at: datetime,
    duration_ms: float,
    gateway_status: int,
    upstream_status: int | None,
    request_payload: dict[str, Any],
    response_body: Any,
    error: str | None,
) -> dict[str, Any]:
    return {
        "request_id": request_id,
        "trace_id": trace_id,
        "event_id": event_id,
        "provider": provider,
        "started_at": started_at.astimezone(timezone.utc),
        "duration_ms": duration_ms,
        "gateway_status": gateway_status,
        "upstream_status": upstream_status,
        "request_payload": request_payload,
        "response_body": response_body,
        "error": error,
    }
