from contextlib import asynccontextmanager
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException, Query, Request
from starlette.responses import Response

from app.core.config import get_settings
from app.providers.client import ProviderClient, ProviderName
from app.providers.circuit_breaker import CircuitOpenError


settings = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    timeout = httpx.Timeout(
        settings.provider_timeout_seconds,
        connect=settings.provider_connect_timeout_seconds,
    )

    limits = httpx.Limits(
        max_connections=200,
        max_keepalive_connections=50,
    )

    async with httpx.AsyncClient(
        timeout=timeout,
        limits=limits,
    ) as http_client:
        app.state.provider_client = ProviderClient(
            client=http_client,
            settings=settings,
        )
        yield


app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    lifespan=lifespan,
)


@app.get("/health", tags=["Health"])
async def health_check() -> dict[str, str]:
    return {
        "status": "ok",
        "service": settings.app_name,
        "version": settings.app_version,
    }


@app.post("/api/v1/process", tags=["Gateway"])
async def process_request(
    request: Request,
    payload: dict[str, Any],
    provider: ProviderName = Query(default="a"),
) -> Response:
    provider_client: ProviderClient = (
        request.app.state.provider_client
    )

    try:
        upstream_response = await provider_client.process(
            provider=provider,
            payload=payload,
        )

    except httpx.TimeoutException as exc:
        raise HTTPException(
            status_code=504,
            detail="Provider request timed out",
        ) from exc

    except httpx.RequestError as exc:
        raise HTTPException(
            status_code=502,
            detail="Failed to communicate with provider",
        ) from exc

    except CircuitOpenError as exc:
        raise HTTPException(
            status_code=503,
            detail="Provider temporarily unavailable",
        ) from exc

    headers = {
        "X-Gateway-Provider": provider,
    }

    content_type = upstream_response.headers.get("content-type")
    if content_type:
        headers["Content-Type"] = content_type

    return Response(
        content=upstream_response.content,
        status_code=upstream_response.status_code,
        headers=headers,
    )
