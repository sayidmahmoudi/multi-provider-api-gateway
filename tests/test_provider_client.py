import unittest
from unittest.mock import AsyncMock, patch

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

    async def test_retry_after_500_when_idempotency_is_enabled(self):
        mock_http = AsyncMock()
        mock_http.post.side_effect = [
            httpx.Response(500),
            httpx.Response(200, json={"status": "ok"}),
        ]

        settings = Settings(
            provider_retry_enabled=True,
            provider_max_retries=2,
            provider_retry_base_delay_seconds=0,
        )

        provider_client = ProviderClient(
            client=mock_http,
            settings=settings,
        )

        with patch(
            "app.providers.client.asyncio.sleep",
            new_callable=AsyncMock,
        ):
            response = await provider_client.process(
                provider="a",
                payload={"amount": 1000},
                idempotency_key="test-request-123",
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(mock_http.post.await_count, 2)

        first_call = mock_http.post.await_args_list[0]
        self.assertEqual(
            first_call.kwargs["headers"]["Idempotency-Key"],
            "test-request-123",
        )

    async def test_retry_is_disabled_without_idempotency_key(self):
        mock_http = AsyncMock()
        mock_http.post.return_value = httpx.Response(500)

        settings = Settings(
            provider_retry_enabled=True,
            provider_max_retries=2,
            provider_retry_base_delay_seconds=0,
        )

        provider_client = ProviderClient(
            client=mock_http,
            settings=settings,
        )

        response = await provider_client.process(
            provider="a",
            payload={"amount": 1000},
        )

        self.assertEqual(response.status_code, 500)
        self.assertEqual(mock_http.post.await_count, 1)


if __name__ == "__main__":
    unittest.main()
