from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "Multi-Provider API Gateway"
    app_version: str = "0.1.0"
    environment: str = "development"

    host: str = "0.0.0.0"
    port: int = 8000
    database_url: str = "postgresql+asyncpg://gateway:gateway@db:5432/gateway"
    redis_url: str = "redis://redis:6379/0"
    redis_key_prefix: str = "gateway"
    audit_stream_max_length: int = 1_000_000
    audit_reservation_ttl_seconds: int = 120
    audit_batch_size: int = 500
    otel_service_name: str = "multi-provider-api-gateway"
    otel_exporter_otlp_endpoint: str = "http://jaeger:4317"
    otel_enabled: bool = True

    service_rps: float = 100.0
    service_burst: int = 200
    service_max_concurrency: int = 100
    capacity_lease_ttl_seconds: int = 120

    provider_a_url: str = "http://92.114.51.251:8001/v1/process"
    provider_b_url: str = "http://92.114.51.251:8002/v1/process"
    provider_c_url: str = "http://92.114.51.251:8003/v1/process"

    provider_timeout_seconds: float = 12.0
    provider_connect_timeout_seconds: float = 2.0

    circuit_breaker_failure_threshold: int = 5
    circuit_breaker_recovery_seconds: float = 15.0

    provider_retry_enabled: bool = False
    provider_max_retries: int = 2
    provider_retry_base_delay_seconds: float = 0.25

    provider_a_rps: float = 5.0
    provider_a_burst: int = 5
    provider_a_max_concurrency: int = 10

    provider_b_rps: float = 10.0
    provider_b_burst: int = 10
    provider_b_max_concurrency: int = 20

    provider_c_rps: float = 10.0
    provider_c_burst: int = 10
    provider_c_max_concurrency: int = 20

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


@lru_cache
def get_settings() -> Settings:
    return Settings()
