# Benchmark results

Start the stack, then run a closed-loop benchmark or specify a fixed offered
rate (open-loop):

```bash
python scripts/benchmark.py --provider a --duration 60 --concurrency 10
python scripts/benchmark.py --provider b --duration 60 --concurrency 20
python scripts/benchmark.py --provider c --duration 60 --concurrency 20
python scripts/benchmark.py --provider a --duration 10 --rps 1000
python scripts/benchmark.py --provider a --duration 5 --rps 10000 --max-in-flight 2000

# Run the load generator separately from the API container for high rates:
docker compose run --rm --no-deps api python scripts/benchmark.py \
  --base-url http://api:8000 --provider b --duration 2 --rps 1000 \
  --max-in-flight 3000
docker compose run --rm --no-deps api python scripts/benchmark.py \
  --base-url http://api:8000 --provider b --duration 1 --rps 10000 \
  --max-in-flight 10000
```

The script reports offered rate, actual request throughput, successful
throughput, average/P95/P99 latency, status counts, error rate, and client-side
drops (when `--max-in-flight` is reached). Closed-loop concurrency is the number
of client workers. Fixed-rate runs are best for checking admission behavior;
they do not represent provider capacity when requests are shed by the gateway.
Run the benchmark from a machine with network access to the mock providers.
With one API container, default limits, and the live mock providers, these
short runs were recorded. Provider A was measured on 2026-10-19; B and C were
measured on 2026-10-20:

| Provider / offered load | Completed | Client drops | Successful | Throughput | Successful RPS | Avg | P95 | P99 | Error rate |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| A, 1,000 RPS for 3s | 2,019 | 981 | 21 | 479.19 RPS | 4.98 RPS | 665.83 ms | 1,011.90 ms | 1,322.45 ms | 98.96% |
| A, 10,000 RPS for 2s | 1,410 | 18,590 | 15 | 436.22 RPS | 4.64 RPS | 740.19 ms | 1,231.64 ms | 1,245.92 ms | 98.94% |
| B, 5 workers for 12s | 13 | 0 | 12 | 0.60 RPS | 0.55 RPS | 6,225.88 ms | 10,407.25 ms | 10,407.25 ms | 7.69% |
| C, 5 workers for 12s | 24 | 0 | 22 | 1.63 RPS | 1.49 RPS | 2,825.97 ms | 4,279.89 ms | 4,304.01 ms | 8.33% |

Provider A stayed near its configured 5 RPS successful limit under overload. The
large 503 count reflects backpressure and provider circuit/capacity rejection;
the fixed-rate client also dropped work after reaching its 500 in-flight cap.
Provider B and C were benchmarked with five closed-loop workers; each returned
intermittent 503 responses among successful calls. These short runs validate
integration and overload behavior; they do not demonstrate 1,000–10,000
successful RPS. The live mock providers, one API container, and deliberately
conservative A limit dominate these measurements.

## Fixed-rate B/C overload runs

The B/C fixed-rate runs below used a separate load-generator container. The
1,000 RPS runs lasted two seconds with a 3,000 in-flight cap; the 10,000 RPS
runs lasted one second with a 10,000 in-flight cap. `Scheduled` is the number
of benchmark tasks that completed, including attempts that ended without an
HTTP response. `No HTTP status` counts attempts where the client received no
response from the gateway. All runs were made on 2026-10-20:

| Provider / offered rate | Scheduled | Client drops | HTTP 200 | HTTP 503 | HTTP 504 | No HTTP status | Throughput | Successful RPS | Avg | P95 | P99 | Error rate |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| B, 1,000 RPS | 2,000 | 0 | 32 | 1,069 | 0 | 899 | 124.65 RPS | 1.99 RPS | 3,684.78 ms | 5,773.17 ms | 6,842.01 ms | 98.40% |
| C, 1,000 RPS | 2,000 | 0 | 39 | 1,033 | 0 | 928 | 186.48 RPS | 3.64 RPS | 4,202.10 ms | 7,218.43 ms | 8,054.01 ms | 98.05% |
| B, 10,000 RPS | 10,000 | 0 | 0 | 0 | 0 | 10,000 | 141.82 RPS | 0.00 RPS | 46,109.31 ms | 65,402.74 ms | 65,687.15 ms | 100.00% |
| C, 10,000 RPS | 10,000 | 0 | 0 | 0 | 0 | 10,000 | 143.93 RPS | 0.00 RPS | 44,593.42 ms | 63,945.31 ms | 64,308.23 ms | 100.00% |

The 1,000 RPS runs scheduled the full offered load, but only about 2–4
successful responses per second were observed; many attempts received 503 or
no HTTP response. At 10,000 RPS the local stack and/or load generator saturated:
all tasks ended without an HTTP response and draining took about 70 seconds.
Those 10,000 RPS rows are overload observations, not evidence that the gateway
accepted 10,000 requests per second or that the upstream providers sustained
that rate. A distributed load generator and a scaled multi-replica environment
are needed to measure that capacity meaningfully.

Earlier pre-Redis closed-loop runs (one API container, five workers) produced:

| Provider / run | Requests | Successful | Attempted RPS | Successful RPS | Avg | P95 | P99 | HTTP errors |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| A, saturation (15s) | 7,093 | 79 | 436.91 | 4.87 | 10.83 ms | 2.11 ms | 599.86 ms | 98.89% |
| A, one worker (6s) | 7 | 7 | 1.14 | 1.14 | 876.16 ms | 1,230.84 ms | 1,230.84 ms | 0.00% |
| B (12s) | 19 | 17 | 0.96 | 0.86 | 4,247.60 ms | 12,005.92 ms | 12,005.92 ms | 10.53% |
| C (12s) | 33 | 24 | 2.15 | 1.57 | 2,125.04 ms | 3,988.29 ms | 4,006.27 ms | 27.27% |

These are short, single-run measurements from 2026-10-19, before Redis-backed
shared controls and durable stream batching, and should not be treated as
capacity claims. Provider A's offered load greatly exceeded its then-configured
5 RPS cap, so most attempts were rejected with 503. B returned one 503 and had
one timeout; C returned nine 503 responses. These figures are historical
context; the measurements above are the results for the current implementation.
