import json
from typing import Any
from uuid import uuid4

from redis.asyncio import Redis


_RESERVE_LUA = """
local t = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2]) / 1000000
redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', now)
local used = redis.call('XLEN', KEYS[1]) + redis.call('ZCARD', KEYS[2])
if used >= tonumber(ARGV[1]) then
    return 0
end
redis.call('ZADD', KEYS[2], now + tonumber(ARGV[2]), ARGV[3])
redis.call('EXPIRE', KEYS[2], math.ceil(tonumber(ARGV[2]) * 2))
return 1
"""

_PUBLISH_LUA = """
local t = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2]) / 1000000
redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', now)
local reserved = redis.call('ZREM', KEYS[2], ARGV[2])
if reserved == 0 and redis.call('XLEN', KEYS[1]) >= tonumber(ARGV[3]) then
    return 0
end
return redis.call('XADD', KEYS[1], '*', 'event', ARGV[1])
"""

_RELEASE_LUA = "return redis.call('ZREM', KEYS[1], ARGV[1])"


class AuditQueueFull(Exception):
    """Raised when the durable audit queue cannot accept another event."""


class AuditEventQueue:
    def __init__(
        self,
        redis: Redis,
        stream: str,
        max_length: int = 1_000_000,
        reservation_ttl_seconds: int = 120,
    ) -> None:
        self._redis = redis
        self._stream = stream
        self._reservations = f"{stream}:reservations"
        self._max_length = max_length
        self._reservation_ttl = reservation_ttl_seconds

    async def reserve(self) -> str:
        token = str(uuid4())
        accepted = await self._redis.eval(
            _RESERVE_LUA,
            2,
            self._stream,
            self._reservations,
            self._max_length,
            self._reservation_ttl,
            token,
        )
        if not accepted:
            raise AuditQueueFull("Audit event queue is at capacity")
        return token

    async def publish(self, token: str, event: dict[str, Any]) -> str:
        serialized = json.dumps(event, separators=(",", ":"), default=str)
        entry_id = await self._redis.eval(
            _PUBLISH_LUA,
            2,
            self._stream,
            self._reservations,
            serialized,
            token,
            self._max_length,
        )
        if not entry_id:
            raise AuditQueueFull("Audit event queue is at capacity")
        return str(entry_id)

    async def release(self, token: str) -> None:
        await self._redis.eval(_RELEASE_LUA, 1, self._reservations, token)
