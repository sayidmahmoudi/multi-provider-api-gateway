from contextlib import asynccontextmanager
import json
import logging
import re
import time
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

import httpx
from redis.asyncio import from_url
from fastapi import FastAPI, HTTPException, Query, Request
from redis.exceptions import RedisError
from starlette.responses import Response

from app.core.config import get_settings
from app.event_queue import AuditEventQueue, AuditQueueFull
from app.providers.client import ProviderClient, ProviderName
from app.providers.circuit_breaker import CircuitOpenError
from app.providers.limiter import (
    ProviderCapacityError,
    ProviderCapacityLimiter,
)
from app.storage import make_request_event


settings = get_settings()
logger = logging.getLogger("gateway")
logger.setLevel(logging.INFO)
logger.propagate = False
if not logger.handlers:
    log_handler = logging.StreamHandler()
    log_handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(log_handler)
_LATENCY_BUCKETS = (0.1, 0.25, 0.5, 1, 2, 5, 10)
_request_counts: dict[tuple[str, int], int] = {}
_latency_counts = [0] * (len(_LATENCY_BUCKETS) + 1)
_latency_sum = 0.0
_tracer_provider = None


def _effective_lease_ttl() -> int:
    return max(
        settings.capacity_lease_ttl_seconds,
        int(
            settings.provider_timeout_seconds
            * (settings.provider_max_retries + 1)
            + 60
        ),
    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    redis = from_url(settings.redis_url, decode_responses=True)
    try:
        await redis.ping()
        app.state.audit_queue = AuditEventQueue(
            redis=redis,
            stream=f"{settings.redis_key_prefix}:audit:events",
            max_length=settings.audit_stream_max_length,
            reservation_ttl_seconds=max(
                settings.audit_reservation_ttl_seconds,
                _effective_lease_ttl(),
            ),
        )
        app.state.service_limiter = ProviderCapacityLimiter(
            rate_per_second=settings.service_rps,
            burst=settings.service_burst,
            max_concurrency=settings.service_max_concurrency,
            redis=redis,
            namespace=f"{settings.redis_key_prefix}:service",
            lease_ttl_seconds=_effective_lease_ttl(),
        )
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
                redis=redis,
            )
            yield
    finally:
        await redis.aclose()
        if _tracer_provider is not None:
            _tracer_provider.shutdown()


app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    lifespan=lifespan,
)


def configure_telemetry() -> None:
    global _tracer_provider
    if not settings.otel_enabled:
        return

    from opentelemetry import trace
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
        OTLPSpanExporter,
    )
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
    from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    tracer_provider = TracerProvider(
        resource=Resource.create({"service.name": settings.otel_service_name})
    )
    tracer_provider.add_span_processor(
        BatchSpanProcessor(
            OTLPSpanExporter(
                endpoint=settings.otel_exporter_otlp_endpoint,
                insecure=True,
            )
        )
    )
    trace.set_tracer_provider(tracer_provider)
    _tracer_provider = tracer_provider
    FastAPIInstrumentor.instrument_app(app, tracer_provider=tracer_provider)
    HTTPXClientInstrumentor().instrument(tracer_provider=tracer_provider)


configure_telemetry()


@app.middleware("http")
async def trace_and_measure_request(request: Request, call_next):
    global _latency_sum

    request_id = request.headers.get("X-Request-ID", "")
    if not re.fullmatch(r"[A-Za-z0-9._:-]{1,128}", request_id):
        request_id = str(uuid4())

    request.state.request_id = request_id
    started = time.perf_counter()
    status_code = 500
    try:
        response = await call_next(request)
        status_code = response.status_code
        if not getattr(request.state, "trace_id", None):
            request.state.trace_id = uuid4().hex
        response.headers["X-Request-ID"] = request_id
        response.headers["X-Trace-ID"] = request.state.trace_id
        return response
    finally:
        elapsed = time.perf_counter() - started
        metric_path = request.url.path
        if metric_path not in {"/health", "/metrics", "/api/v1/process"}:
            metric_path = "unmatched"
        _request_counts[(metric_path, status_code)] = (
            _request_counts.get((metric_path, status_code), 0) + 1
        )
        _latency_sum += elapsed
        for index, upper_bound in enumerate(_LATENCY_BUCKETS):
            if elapsed <= upper_bound:
                _latency_counts[index] += 1
        _latency_counts[-1] += 1
        logger.info(
            json.dumps(
                {
                    "event": "http_request",
                    "request_id": request_id,
                    "trace_id": getattr(request.state, "trace_id", ""),
                    "method": request.method,
                    "path": request.url.path,
                    "status_code": status_code,
                    "duration_seconds": round(elapsed, 6),
                },
                separators=(",", ":"),
            )
        )


@app.get("/health", tags=["Health"])
async def health_check() -> dict[str, str]:
    return {
        "status": "ok",
        "service": settings.app_name,
        "version": settings.app_version,
    }


@app.get("/metrics", include_in_schema=False)
async def metrics() -> Response:
    lines = ["# TYPE gateway_http_requests_total counter"]
    for (path, status), count in sorted(_request_counts.items()):
        lines.append(
            f'gateway_http_requests_total{{path="{path}",status="{status}"}} {count}'
        )

    lines.extend(
        [
            "# TYPE gateway_http_request_duration_seconds histogram",
        ]
    )
    for index, upper_bound in enumerate(_LATENCY_BUCKETS):
        lines.append(
            f'gateway_http_request_duration_seconds_bucket{{le="{upper_bound}"}} {_latency_counts[index]}'
        )
    lines.append(
        f'gateway_http_request_duration_seconds_bucket{{le="+Inf"}} {_latency_counts[-1]}'
    )
    lines.append(
        f"gateway_http_request_duration_seconds_sum {_latency_sum}"
    )
    lines.append(
        f"gateway_http_request_duration_seconds_count {_latency_counts[-1]}"
    )
    return Response(
        content="\n".join(lines) + "\n",
        media_type="text/plain; version=0.0.4; charset=utf-8",
    )


@app.post("/api/v1/process", tags=["Gateway"])
async def process_request(
    request: Request,
    payload: dict[str, Any],
    provider: ProviderName = Query(default="a"),
) -> Response:
    from opentelemetry import trace

    span_context = trace.get_current_span().get_span_context()
    request.state.trace_id = (
        f"{span_context.trace_id:032x}"
        if span_context.is_valid
        else uuid4().hex
    )
    provider_client: ProviderClient = (
        request.app.state.provider_client
    )

    started_at = datetime.now(timezone.utc)
    started_clock = time.perf_counter()
    upstream_response: httpx.Response | None = None
    gateway_status = 500
    error: str | None = None
    service_limiter: ProviderCapacityLimiter = (
        request.app.state.service_limiter
    )
    service_lease: str | None = None
    audit_reservation: str | None = None
    try:
        audit_reservation = await request.app.state.audit_queue.reserve()
        service_lease = await service_limiter.acquire()
        if service_lease is None:
            gateway_status = 503
            error = "ServiceCapacityError"
            raise HTTPException(
                status_code=503,
                detail="Gateway capacity temporarily exhausted",
                headers={"Retry-After": "1"},
            )

        idempotency_key = request.headers.get("Idempotency-Key")

        upstream_response = await provider_client.process(
            provider=provider,
            payload=payload,
            idempotency_key=idempotency_key,
            request_id=request.state.request_id,
            trace_id=request.state.trace_id,
        )
        gateway_status = upstream_response.status_code

    except httpx.TimeoutException as exc:
        gateway_status = 504
        error = type(exc).__name__
        raise HTTPException(
            status_code=504,
            detail="Provider request timed out",
        ) from exc

    except httpx.RequestError as exc:
        gateway_status = 502
        error = type(exc).__name__
        raise HTTPException(
            status_code=502,
            detail="Failed to communicate with provider",
        ) from exc

    except CircuitOpenError as exc:
        gateway_status = 503
        error = type(exc).__name__
        raise HTTPException(
            status_code=503,
            detail="Provider temporarily unavailable",
        ) from exc

    except ProviderCapacityError as exc:
        gateway_status = 503
        error = type(exc).__name__
        raise HTTPException(
            status_code=503,
            detail="Provider capacity temporarily exhausted",
            headers={"Retry-After": "1"},
        ) from exc
    except AuditQueueFull as exc:
        gateway_status = 503
        error = type(exc).__name__
        raise HTTPException(
            status_code=503,
            detail="Audit queue capacity temporarily exhausted",
            headers={"Retry-After": "1"},
        ) from exc
    except RedisError as exc:
        gateway_status = 503
        error = "RedisUnavailable"
        raise HTTPException(
            status_code=503,
            detail="Shared gateway services temporarily unavailable",
            headers={"Retry-After": "1"},
        ) from exc
    finally:
        if service_lease is not None:
            await service_limiter.release(service_lease)
        body: Any = None
        upstream_status = None
        if upstream_response is not None:
            upstream_status = upstream_response.status_code
            try:
                body = upstream_response.json()
            except ValueError:
                body = {"text": upstream_response.text[:4096]}

        event = make_request_event(
                event_id="",
                request_id=request.state.request_id,
                trace_id=getattr(request.state, "trace_id", ""),
                provider=provider,
                started_at=started_at,
                duration_ms=(time.perf_counter() - started_clock) * 1000,
                gateway_status=gateway_status,
                upstream_status=upstream_status,
                request_payload=payload,
                response_body=body,
                error=error,
            )
        if audit_reservation is not None:
            try:
                await request.app.state.audit_queue.publish(
                    audit_reservation,
                    event,
                )
            except Exception:
                logger.exception(
                    "audit_event_publish_failed",
                    extra={"request_id": request.state.request_id},
                )

    headers = {
        "X-Gateway-Provider": provider,
        "X-Trace-ID": request.state.trace_id,
    }

    content_type = upstream_response.headers.get("content-type")
    if content_type:
        headers["Content-Type"] = content_type

    return Response(
        content=upstream_response.content,
        status_code=upstream_response.status_code,
        headers=headers,
    )
