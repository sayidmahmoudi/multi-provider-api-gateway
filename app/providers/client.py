from typing import Any, Literal

import httpx

from app.core.config import Settings
from app.providers.circuit_breaker import CircuitBreaker


ProviderName = Literal["a", "b", "c"]


class ProviderClient:
    def __init__(
        self,
        client: httpx.AsyncClient,
        settings: Settings,
    ) -> None:
        self._client = client

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
    ) -> httpx.Response:
        breaker = self._breakers[provider]

        permit = await breaker.before_request()

        try:
            response = await self._client.post(
                self._provider_urls[provider],
                json=payload,
            )
        except httpx.RequestError:
            await breaker.record_failure(permit)
            raise

        if response.status_code >= 500:
            await breaker.record_failure(permit)
        else:
            await breaker.record_success(permit)

        return response
