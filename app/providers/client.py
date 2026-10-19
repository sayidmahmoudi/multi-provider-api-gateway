from typing import Any, Literal

import httpx

from app.core.config import Settings


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

    async def process(
        self,
        provider: ProviderName,
        payload: dict[str, Any],
    ) -> httpx.Response:
        url = self._provider_urls[provider]

        response = await self._client.post(
            url,
            json=payload,
        )

        return response
