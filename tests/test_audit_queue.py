import unittest
from unittest.mock import AsyncMock

from app.event_queue import AuditEventQueue, AuditQueueFull


class AuditEventQueueTests(unittest.IsolatedAsyncioTestCase):
    async def test_reservation_and_publish_are_atomic_redis_operations(self):
        redis = AsyncMock()
        redis.eval.side_effect = [1, "1712345678901-0"]
        queue = AuditEventQueue(redis, "test:events", max_length=4)

        token = await queue.reserve()
        entry_id = await queue.publish(token, {"request_id": "req-1"})

        self.assertEqual(entry_id, "1712345678901-0")
        self.assertEqual(redis.eval.await_count, 2)
        self.assertEqual(
            redis.eval.await_args_list[0].args[2:4],
            ("test:events", "test:events:reservations"),
        )

    async def test_full_stream_rejects_before_forwarding(self):
        redis = AsyncMock()
        redis.eval.return_value = 0
        queue = AuditEventQueue(redis, "test:events", max_length=1)

        with self.assertRaises(AuditQueueFull):
            await queue.reserve()


if __name__ == "__main__":
    unittest.main()
