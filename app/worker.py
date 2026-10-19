import asyncio
import json
import logging
import socket
from datetime import datetime
from typing import Any
from uuid import uuid4

from redis.asyncio import from_url
from sqlalchemy.ext.asyncio import create_async_engine

from app.core.config import get_settings
from app.storage import persist_request_events


logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger("gateway.audit_worker")


async def _claim_pending(redis, stream: str, group: str, consumer: str, count: int):
    result = await redis.xautoclaim(
        stream,
        group,
        consumer,
        min_idle_time=10_000,
        start_id="0-0",
        count=count,
    )
    return result[1]


async def _read_new(redis, stream: str, group: str, consumer: str, count: int):
    result = await redis.xreadgroup(
        group,
        consumer,
        {stream: ">"},
        count=count,
        block=500,
    )
    if not result:
        return []
    return result[0][1]


async def _run() -> None:
    settings = get_settings()
    redis = from_url(settings.redis_url, decode_responses=True)
    engine = create_async_engine(
        settings.database_url,
        pool_size=10,
        max_overflow=20,
        pool_pre_ping=True,
    )
    stream = f"{settings.redis_key_prefix}:audit:events"
    group = "postgres-writers"
    consumer = f"{socket.gethostname()}-{uuid4()}"
    batch_size = settings.audit_batch_size

    try:
        await redis.ping()
        try:
            await redis.xgroup_create(stream, group, id="0-0", mkstream=True)
        except Exception as exc:
            if "BUSYGROUP" not in str(exc):
                raise

        logger.info(json.dumps({"event": "audit_worker_started", "consumer": consumer}))
        while True:
            messages = await _claim_pending(
                redis, stream, group, consumer, batch_size
            )
            if not messages:
                messages = await _read_new(
                    redis, stream, group, consumer, batch_size
                )
            if not messages:
                continue

            valid: list[dict[str, Any]] = []
            ack_ids: list[str] = []
            for message_id, fields in messages:
                try:
                    event = json.loads(fields["event"])
                    event["event_id"] = message_id
                    event["started_at"] = datetime.fromisoformat(
                        event["started_at"]
                    )
                    valid.append(event)
                    ack_ids.append(message_id)
                except (KeyError, TypeError, ValueError, json.JSONDecodeError):
                    logger.exception(
                        "invalid_audit_event",
                        extra={"message_id": message_id},
                    )
                    ack_ids.append(message_id)

            try:
                if valid:
                    await persist_request_events(engine, valid)
                if ack_ids:
                    async with redis.pipeline(transaction=True) as pipeline:
                        pipeline.xack(stream, group, *ack_ids)
                        pipeline.xdel(stream, *ack_ids)
                        await pipeline.execute()
            except Exception:
                logger.exception(
                    "audit_batch_write_failed",
                    extra={"batch_size": len(valid)},
                )
                await asyncio.sleep(1)
    finally:
        await redis.aclose()
        await engine.dispose()


if __name__ == "__main__":
    try:
        asyncio.run(_run())
    except KeyboardInterrupt:
        pass
