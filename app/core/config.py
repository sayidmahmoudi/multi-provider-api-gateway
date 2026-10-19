from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "Multi-Provider API Gateway"
    app_version: str = "0.1.0"
    environment: str = "development"

    host: str = "0.0.0.0"
    port: int = 8000

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

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


@lru_cache
def get_settings() -> Settings:
    return Settings()
