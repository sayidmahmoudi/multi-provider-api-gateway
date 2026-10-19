
import asyncio
from typing import Any, Literal

import httpx

from app.core.config import Settings
from app.providers.circuit_breaker import (
    CircuitBreaker,
)

ProviderName = Literal["a", "b", "c"]


class ProviderClient:
    def __init__(
        self,
        client: httpx.AsyncClient,
        settings: Settings,
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

    async def process(
        self,
        provider: ProviderName,
        payload: dict[str, Any],
        idempotency_key: str | None = None,
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

        for attempt in range(max_attempts):
            permit = await breaker.before_request()

            try:
                response = await self._client.post(
                    self._provider_urls[provider],
                    json=payload,
                    headers=headers or None,
                )
            except httpx.RequestError:
                await breaker.record_failure(permit)

                if attempt + 1 >= max_attempts:
                    raise

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
