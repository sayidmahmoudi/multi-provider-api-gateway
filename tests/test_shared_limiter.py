import unittest
from unittest.mock import AsyncMock

from app.providers.limiter import ProviderCapacityLimiter


class SharedProviderCapacityLimiterTests(unittest.IsolatedAsyncioTestCase):
    async def test_redis_backed_limiters_use_same_atomic_key(self):
        redis = AsyncMock()
        redis.eval.side_effect = [1, 0, 1]
        first = ProviderCapacityLimiter(
            rate_per_second=5,
            burst=3,
            max_concurrency=2,
            redis=redis,
            namespace="test:provider:a",
        )
        second = ProviderCapacityLimiter(
            rate_per_second=5,
            burst=3,
            max_concurrency=2,
            redis=redis,
            namespace="test:provider:a",
        )

        lease = await first.acquire()
        self.assertIsNotNone(lease)
        self.assertIsNone(await second.acquire())
        await first.release(lease)

        self.assertEqual(redis.eval.await_count, 3)
        self.assertEqual(redis.eval.await_args_list[0].args[2:4], (
            "test:provider:a",
            "test:provider:a:leases",
        ))
        self.assertEqual(redis.eval.await_args_list[1].args[2:4], (
            "test:provider:a",
            "test:provider:a:leases",
        ))


if __name__ == "__main__":
    unittest.main()
