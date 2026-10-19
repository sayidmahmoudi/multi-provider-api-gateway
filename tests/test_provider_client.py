import unittest
from unittest.mock import AsyncMock

import httpx

from app.core.config import Settings
from app.providers.client import ProviderClient
from app.providers.circuit_breaker import CircuitOpenError


class ProviderClientTests(unittest.IsolatedAsyncioTestCase):
    async def test_open_circuit_prevents_upstream_request(self):
        mock_http = AsyncMock()
        mock_http.post.return_value = httpx.Response(500)

        settings = Settings(
            circuit_breaker_failure_threshold=5,
            circuit_breaker_recovery_seconds=30,
        )

        provider_client = ProviderClient(
            client=mock_http,
            settings=settings,
        )

        payload = {"amount": 1000, "currency": "IRR"}

        # Five upstream 500 responses should open the circuit.
        for _ in range(5):
            response = await provider_client.process("a", payload)
            self.assertEqual(response.status_code, 500)

        self.assertEqual(mock_http.post.await_count, 5)

        # The sixth request must be rejected before reaching the provider.
        with self.assertRaises(CircuitOpenError):
            await provider_client.process("a", payload)

        self.assertEqual(mock_http.post.await_count, 5)

    async def test_circuit_breakers_are_independent_per_provider(self):
        mock_http = AsyncMock()
        mock_http.post.return_value = httpx.Response(500)

        settings = Settings(
            circuit_breaker_failure_threshold=5,
            circuit_breaker_recovery_seconds=30,
        )

        provider_client = ProviderClient(
            client=mock_http,
            settings=settings,
        )

        payload = {"amount": 1000}

        for _ in range(5):
            await provider_client.process("a", payload)

        # Provider A is open, but Provider B must remain callable.
        response = await provider_client.process("b", payload)

        self.assertEqual(response.status_code, 500)
        self.assertEqual(mock_http.post.await_count, 6)


if __name__ == "__main__":
    unittest.main()