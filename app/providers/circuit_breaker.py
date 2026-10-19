import asyncio
import time
from dataclasses import dataclass
from enum import Enum


class CircuitState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitOpenError(Exception):
    """Raised when the circuit does not allow a request."""


@dataclass(frozen=True)
class RequestPermit:
    generation: int
    is_probe: bool = False


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
        self.generation = 0

        self._lock = asyncio.Lock()

    async def before_request(self) -> RequestPermit:
        async with self._lock:
            if self.state == CircuitState.OPEN:
                elapsed = time.monotonic() - self.opened_at

                if elapsed < self.recovery_seconds:
                    raise CircuitOpenError(
                        "Circuit breaker is open"
                    )

                self.state = CircuitState.HALF_OPEN
                self.generation += 1

            if self.state == CircuitState.HALF_OPEN:
                if self.probe_in_flight:
                    raise CircuitOpenError(
                        "Circuit breaker is waiting for a probe"
                    )

                self.probe_in_flight = True

                return RequestPermit(
                    generation=self.generation,
                    is_probe=True,
                )

            return RequestPermit(
                generation=self.generation,
            )

    async def record_success(
        self,
        permit: RequestPermit,
    ) -> None:
        async with self._lock:
            if permit.generation != self.generation:
                return

            if self.state == CircuitState.OPEN:
                return

            if self.state == CircuitState.HALF_OPEN:
                if not permit.is_probe:
                    return

                self.state = CircuitState.CLOSED
                self.failure_count = 0
                self.probe_in_flight = False
                self.generation += 1
                return

            self.failure_count = 0

    async def record_failure(
        self,
        permit: RequestPermit,
    ) -> None:
        async with self._lock:
            if permit.generation != self.generation:
                return

            if self.state == CircuitState.OPEN:
                return

            if self.state == CircuitState.HALF_OPEN:
                if not permit.is_probe:
                    return

                self._open()
                return

            self.failure_count += 1

            if self.failure_count >= self.failure_threshold:
                self._open()

    def _open(self) -> None:
        self.state = CircuitState.OPEN
        self.opened_at = time.monotonic()
        self.probe_in_flight = False
        self.generation += 1