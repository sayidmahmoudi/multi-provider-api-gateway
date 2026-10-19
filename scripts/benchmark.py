#!/usr/bin/env python3
"""Closed-loop or fixed-rate load benchmark for the gateway endpoint."""

import argparse
import asyncio
import math
import time

import httpx


def percentile(values: list[float], percent: float) -> float:
    ordered = sorted(values)
    return ordered[max(0, math.ceil(percent * len(ordered)) - 1)]


async def run(args: argparse.Namespace) -> None:
    url = f"{args.base_url.rstrip('/')}/api/v1/process?provider={args.provider}"
    latencies: list[float] = []
    statuses: list[int] = []
    errors = 0
    dropped = 0

    limits = httpx.Limits(
        max_connections=args.max_in_flight,
        max_keepalive_connections=min(args.max_in_flight, 100),
    )
    async with httpx.AsyncClient(timeout=args.timeout, limits=limits) as client:
        async def send(worker_id: int, request_number: int) -> None:
            nonlocal errors
            started = time.perf_counter()
            try:
                response = await client.post(
                    url,
                    json={"benchmark": True, "worker": worker_id},
                    headers={
                        "Idempotency-Key": f"bench-{worker_id}-{request_number}"
                    },
                )
                statuses.append(response.status_code)
                if response.status_code >= 400:
                    errors += 1
            except httpx.HTTPError:
                errors += 1
            finally:
                latencies.append(time.perf_counter() - started)

        started = time.perf_counter()
        if args.rps:
            deadline = started + args.duration
            active: set[asyncio.Task[None]] = set()
            request_number = 0
            interval = 1 / args.rps
            while True:
                scheduled_at = started + request_number * interval
                if scheduled_at >= deadline:
                    break
                delay = scheduled_at - time.perf_counter()
                if delay > 0:
                    await asyncio.sleep(delay)
                active = {task for task in active if not task.done()}
                if len(active) >= args.max_in_flight:
                    dropped += 1
                else:
                    task = asyncio.create_task(send(0, request_number))
                    active.add(task)
                request_number += 1
            if active:
                await asyncio.gather(*active)
        else:
            deadline = time.perf_counter() + args.duration

            async def worker(worker_id: int) -> None:
                request_number = 0
                while time.perf_counter() < deadline:
                    await send(worker_id, request_number)
                    request_number += 1

            await asyncio.gather(
                *(worker(worker_id) for worker_id in range(args.concurrency))
            )
        elapsed = time.perf_counter() - started

    count = len(latencies)
    successful = sum(200 <= status < 400 for status in statuses)
    print(f"requests: {count}")
    print(f"client_dropped: {dropped}")
    print(f"offered_rps: {args.rps:.2f}" if args.rps else "offered_rps: closed-loop")
    print(f"duration_seconds: {elapsed:.2f}")
    print(f"throughput_rps: {count / elapsed:.2f}")
    print(f"successful_rps: {successful / elapsed:.2f}")
    print(f"error_rate: {errors / count:.2%}" if count else "error_rate: n/a")
    print(f"latency_avg_ms: {sum(latencies) / count * 1000:.2f}" if count else "latency_avg_ms: n/a")
    print(f"latency_p95_ms: {percentile(latencies, 0.95) * 1000:.2f}" if count else "latency_p95_ms: n/a")
    print(f"latency_p99_ms: {percentile(latencies, 0.99) * 1000:.2f}" if count else "latency_p99_ms: n/a")
    print("status_counts:")
    for status, status_count in sorted(
        ((status, statuses.count(status)) for status in set(statuses))
    ):
        print(f"  {status}: {status_count}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://localhost:8000")
    parser.add_argument("--provider", choices=("a", "b", "c"), default="a")
    parser.add_argument("--duration", type=float, default=30)
    parser.add_argument("--concurrency", type=int, default=10)
    parser.add_argument("--timeout", type=float, default=30)
    parser.add_argument("--rps", type=float, default=0, help="Open-loop offered rate")
    parser.add_argument("--max-in-flight", type=int, default=2000)
    args = parser.parse_args()
    if args.duration <= 0 or args.concurrency < 1 or args.max_in_flight < 1:
        parser.error("duration, concurrency, and max-in-flight must be positive")
    if args.rps < 0:
        parser.error("rps cannot be negative")
    return args


if __name__ == "__main__":
    asyncio.run(run(parse_args()))
