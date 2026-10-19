import asyncio
from typing import Any, Literal

import httpx
from redis.asyncio import Redis

from app.core.config import Settings
from app.providers.circuit_breaker import (
    CircuitBreaker,
)
from app.providers.limiter import (
    ProviderCapacityLimiter,
    ProviderCapacityError,
)

ProviderName = Literal["a", "b", "c"]


class ProviderClient:
    def __init__(
        self,
        client: httpx.AsyncClient,
        settings: Settings,
        redis: Redis | None = None,
    ) -> None:
        self._client = client
        self._settings = settings

        self._provider_urls: dict[ProviderName, str] = {
            "a": settings.provider_a_url,
            "b": settings.provider_b_url,
            "c": settings.provider_c_url,
        }

        self._breakers = {
            provider: CircuitBreaker(
                failure_threshold=(
                    settings.circuit_breaker_failure_threshold
                ),
                recovery_seconds=(
                    settings.circuit_breaker_recovery_seconds
                ),
            )
            for provider in self._provider_urls
        }

        self._limiters = {
            "a": ProviderCapacityLimiter(
                rate_per_second=settings.provider_a_rps,
                burst=settings.provider_a_burst,
                max_concurrency=settings.provider_a_max_concurrency,
                redis=redis,
                namespace=f"{settings.redis_key_prefix}:provider:a",
                lease_ttl_seconds=max(
                    settings.capacity_lease_ttl_seconds,
                    int(settings.provider_timeout_seconds * (settings.provider_max_retries + 1) + 60),
                ),
            ),
            "b": ProviderCapacityLimiter(
                rate_per_second=settings.provider_b_rps,
                burst=settings.provider_b_burst,
                max_concurrency=settings.provider_b_max_concurrency,
                redis=redis,
                namespace=f"{settings.redis_key_prefix}:provider:b",
                lease_ttl_seconds=max(
                    settings.capacity_lease_ttl_seconds,
                    int(settings.provider_timeout_seconds * (settings.provider_max_retries + 1) + 60),
                ),
            ),
            "c": ProviderCapacityLimiter(
                rate_per_second=settings.provider_c_rps,
                burst=settings.provider_c_burst,
                max_concurrency=settings.provider_c_max_concurrency,
                redis=redis,
                namespace=f"{settings.redis_key_prefix}:provider:c",
                lease_ttl_seconds=max(
                    settings.capacity_lease_ttl_seconds,
                    int(settings.provider_timeout_seconds * (settings.provider_max_retries + 1) + 60),
                ),
            ),
        }

    async def process(
        self,
        provider: ProviderName,
        payload: dict[str, Any],
        idempotency_key: str | None = None,
        request_id: str | None = None,
        trace_id: str | None = None,
    ) -> httpx.Response:
        breaker = self._breakers[provider]

        # Retry is allowed only when explicitly enabled
        # and an idempotency key is provided.
        retry_allowed = (
            self._settings.provider_retry_enabled
            and bool(idempotency_key)
        )

        max_attempts = (
            1 + self._settings.provider_max_retries
            if retry_allowed
            else 1
        )

        headers: dict[str, str] = {}
        if idempotency_key:
            headers["Idempotency-Key"] = idempotency_key
        if request_id:
            headers["X-Request-ID"] = request_id
        if trace_id:
            headers["X-Trace-ID"] = trace_id

        for attempt in range(max_attempts):
            permit = await breaker.before_request()

            limiter = self._limiters[provider]
            lease = await limiter.acquire()
            if lease is None:
                raise ProviderCapacityError(
                    f"Provider {provider} capacity is exhausted"
                )

            retry_request_error = False
            try:
                try:
                    response = await self._client.post(
                        self._provider_urls[provider],
                        json=payload,
                        headers=headers or None,
                    )
                except asyncio.CancelledError:
                    await breaker.record_failure(permit)
                    raise
                except httpx.RequestError:
                    await breaker.record_failure(permit)
                    if attempt + 1 >= max_attempts:
                        raise
                    retry_request_error = True
            finally:
                await limiter.release(lease)

            if retry_request_error:
                await self._wait_before_retry(attempt)
                continue

            if response.status_code >= 500:
                await breaker.record_failure(permit)

                if attempt + 1 < max_attempts:
                    await self._wait_before_retry(attempt)
                    continue
            else:
                # 4xx responses, including 429, are not retried.
                await breaker.record_success(permit)

            return response

        # The loop returns a response or raises on its final attempt.
        raise RuntimeError("Retry loop exited unexpectedly")

    async def _wait_before_retry(self, attempt: int) -> None:
        delay = (
            self._settings.provider_retry_base_delay_seconds
            * (2 ** attempt)
        )
        await asyncio.sleep(delay)
