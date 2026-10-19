import asyncio
import time
from enum import Enum


class CircuitState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitOpenError(Exception):
    """Raised when a provider circuit does not allow a request."""


class CircuitBreaker:
    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_seconds: float = 15.0,
    ) -> None:
        self.failure_threshold = failure_threshold
        self.recovery_seconds = recovery_seconds

        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.opened_at = 0.0
        self.probe_in_flight = False

        self._lock = asyncio.Lock()

    async def before_request(self) -> None:
        async with self._lock:
            if self.state == CircuitState.OPEN:
                elapsed = time.monotonic() - self.opened_at

                if elapsed < self.recovery_seconds:
                    raise CircuitOpenError(
                        "Circuit breaker is open"
                    )

                self.state = CircuitState.HALF_OPEN

            if self.state == CircuitState.HALF_OPEN:
                if self.probe_in_flight:
                    raise CircuitOpenError(
                        "Circuit breaker is waiting for a probe"
                    )

                self.probe_in_flight = True

    async def record_success(self) -> None:
        async with self._lock:
            self.state = CircuitState.CLOSED
            self.failure_count = 0
            self.probe_in_flight = False

    async def record_failure(self) -> None:
        async with self._lock:
            self.probe_in_flight = False

            if self.state == CircuitState.HALF_OPEN:
                self._open()
                return

            if self.state == CircuitState.OPEN:
                return

            self.failure_count += 1

            if self.failure_count >= self.failure_threshold:
                self._open()

    def _open(self) -> None:
        self.state = CircuitState.OPEN
        self.opened_at = time.monotonic()
        self.probe_in_flight = False
