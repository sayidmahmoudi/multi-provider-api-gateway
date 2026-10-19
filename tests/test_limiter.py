
import unittest

from app.providers.limiter import ProviderCapacityLimiter


class ProviderCapacityLimiterTests(unittest.IsolatedAsyncioTestCase):
    async def test_burst_capacity_is_enforced(self):
        limiter = ProviderCapacityLimiter(
            rate_per_second=1,
            burst=2,
            max_concurrency=10,
        )

        self.assertTrue(await limiter.try_acquire())
        self.assertTrue(await limiter.try_acquire())
        self.assertFalse(await limiter.try_acquire())

        await limiter.release()
        await limiter.release()

    async def test_concurrency_limit_is_enforced(self):
        limiter = ProviderCapacityLimiter(
            rate_per_second=100,
            burst=10,
            max_concurrency=1,
        )

        self.assertTrue(await limiter.try_acquire())
        self.assertFalse(await limiter.try_acquire())

        await limiter.release()

        self.assertTrue(await limiter.try_acquire())
        await limiter.release()


if __name__ == "__main__":
    unittest.main()
