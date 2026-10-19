import asyncio
import unittest

from app.providers.circuit_breaker import (
    CircuitBreaker,
    CircuitOpenError,
    CircuitState,
)


class CircuitBreakerTests(unittest.IsolatedAsyncioTestCase):
    async def test_opens_after_failure_threshold(self):
        breaker = CircuitBreaker(
            failure_threshold=2,
            recovery_seconds=10,
        )

        for _ in range(2):
            permit = await breaker.before_request()
            await breaker.record_failure(permit)

        self.assertEqual(breaker.state, CircuitState.OPEN)

        with self.assertRaises(CircuitOpenError):
            await breaker.before_request()

    async def test_successful_probe_closes_circuit(self):
        breaker = CircuitBreaker(
            failure_threshold=1,
            recovery_seconds=0.01,
        )

        permit = await breaker.before_request()
        await breaker.record_failure(permit)

        await asyncio.sleep(0.02)

        probe = await breaker.before_request()
        self.assertTrue(probe.is_probe)
        self.assertEqual(breaker.state, CircuitState.HALF_OPEN)

        await breaker.record_success(probe)

        self.assertEqual(breaker.state, CircuitState.CLOSED)
        self.assertEqual(breaker.failure_count, 0)

    async def test_only_one_half_open_probe_is_allowed(self):
        breaker = CircuitBreaker(
            failure_threshold=1,
            recovery_seconds=0.01,
        )

        permit = await breaker.before_request()
        await breaker.record_failure(permit)

        await asyncio.sleep(0.02)

        probe = await breaker.before_request()

        with self.assertRaises(CircuitOpenError):
            await breaker.before_request()

        await breaker.record_success(probe)

    async def test_stale_success_cannot_close_newly_opened_circuit(self):
        breaker = CircuitBreaker(
            failure_threshold=1,
            recovery_seconds=10,
        )

        old_permit = await breaker.before_request()
        await breaker.record_failure(old_permit)

        self.assertEqual(breaker.state, CircuitState.OPEN)

        # Simulate a late success from an older in-flight request.
        await breaker.record_success(old_permit)

        self.assertEqual(breaker.state, CircuitState.OPEN)


if __name__ == "__main__":
    unittest.main()
