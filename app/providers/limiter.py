
import asyncio
import logging
import time
from uuid import uuid4
from redis.asyncio import Redis


_ACQUIRE_LUA = """
local t = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2]) / 1000000
local tokens = tonumber(redis.call('HGET', KEYS[1], 'tokens')) or tonumber(ARGV[2])
local updated = tonumber(redis.call('HGET', KEYS[1], 'updated')) or now
tokens = math.min(tonumber(ARGV[2]), tokens + math.max(0, now - updated) * tonumber(ARGV[1]))
redis.call('HSET', KEYS[1], 'tokens', tokens, 'updated', now)
redis.call('EXPIRE', KEYS[1], tonumber(ARGV[4]))
redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', now)
local in_flight = redis.call('ZCARD', KEYS[2])
redis.call('EXPIRE', KEYS[2], tonumber(ARGV[4]))
if tokens < 1 or in_flight >= tonumber(ARGV[3]) then
    return 0
end
redis.call('HSET', KEYS[1], 'tokens', tokens - 1)
redis.call('ZADD', KEYS[2], now + tonumber(ARGV[4]), ARGV[5])
return 1
"""

_RELEASE_LUA = """
return redis.call('ZREM', KEYS[1], ARGV[1])
"""


class ProviderCapacityError(Exception):
    """Raised when a provider's local capacity is exhausted."""


logger = logging.getLogger("gateway.limiter")


class ProviderCapacityLimiter:
    """Non-blocking token-bucket and concurrency limiter."""

    def __init__(
        self,
        rate_per_second: float,
        burst: int,
        max_concurrency: int,
        redis: Redis | None = None,
        namespace: str = "gateway:limiter",
        lease_ttl_seconds: int = 120,
    ) -> None:
        if rate_per_second <= 0:
            raise ValueError("rate_per_second must be positive")
        if burst < 1:
            raise ValueError("burst must be at least 1")
        if max_concurrency < 1:
            raise ValueError("max_concurrency must be at least 1")

        self.rate_per_second = rate_per_second
        self.burst = burst
        self.max_concurrency = max_concurrency
        self._redis = redis
        self._redis_key = namespace
        self._leases_key = f"{namespace}:leases"
        self._lease_ttl = lease_ttl_seconds

        self._tokens = float(burst)
        self._updated_at = time.monotonic()
        self._in_flight = 0
        self._lock = asyncio.Lock()
        self._legacy_tokens: list[str] = []

    async def try_acquire(self) -> bool:
        if self._redis is not None:
            token = await self.acquire()
            if token is None:
                return False
            async with self._lock:
                self._legacy_tokens.append(token)
            return True

        async with self._lock:
            now = time.monotonic()
            elapsed = now - self._updated_at

            self._tokens = min(
                float(self.burst),
                self._tokens + elapsed * self.rate_per_second,
            )
            self._updated_at = now

            if self._tokens < 1:
                return False

            if self._in_flight >= self.max_concurrency:
                return False

            self._tokens -= 1
            self._in_flight += 1
            return True

    async def acquire(self) -> str | None:
        token = str(uuid4())
        if self._redis is None:
            if await self.try_acquire():
                return token
            return None

        ttl = max(
            self._lease_ttl,
            int(self.burst / self.rate_per_second) * 10,
        )
        try:
            acquired = await self._redis.eval(
                _ACQUIRE_LUA,
                2,
                self._redis_key,
                self._leases_key,
                self.rate_per_second,
                self.burst,
                self.max_concurrency,
                ttl,
                token,
            )
        except Exception as exc:
            raise ProviderCapacityError(
                "Shared capacity limiter is unavailable"
            ) from exc
        return token if acquired else None

    async def release(self, token: str | None = None) -> None:
        if self._redis is not None:
            if token is None:
                async with self._lock:
                    if not self._legacy_tokens:
                        raise ValueError("A shared limiter lease token is required")
                    token = self._legacy_tokens.pop()
            try:
                released = await self._redis.eval(
                    _RELEASE_LUA,
                    1,
                    self._leases_key,
                    token,
                )
            except Exception:
                logger.exception("shared_limiter_release_failed")
                return
            if not released:
                raise RuntimeError("Limiter release without acquire")
            return

        async with self._lock:
            if self._in_flight <= 0:
                raise RuntimeError("Limiter release without acquire")

            self._in_flight -= 1
