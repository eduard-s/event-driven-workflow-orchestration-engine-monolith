**The Go monolith was faster and used less memory in these local measurements.**

The result compares complete deployments under one selected resource layout. It includes the language, process arrangement, transport, and implementation differences. It does not isolate Go from Python.

Both engines recovered all five tested process-failure cases. These observations do not establish a general failure rate or a universal reliability winner.

The runs used Docker Desktop on an ARM64 Mac. Docker reported 12 CPUs and about 7.65 GiB of available memory. Other local services remained active. These are local measurements, not a production capacity guarantee.

**Each workload used three rounds with alternating engine order.**

The table reports median throughput and the median of each round's p95 workflow latency. Latency starts when a request enters its client concurrency slot. It includes submission, trigger acceptance, overload retries, and result polling. It excludes time before that client slot.

| Workload | Go workflows/s | Python workflows/s | Go throughput ratio | Go p95 ms | Python p95 ms |
|---|---:|---:|---:|---:|---:|
| Three-node linear graph | 147.87 | 24.78 | 5.97× | 223.2 | 1002.4 |
| Three parallel one-second mocks | 1.32 | 1.28 | 1.03× | 9087.7 | 9491.5 |
| 50-node chain | 25.34 | 1.79 | 14.16× | 375.7 | 4718.3 |
| 192 KiB output | 135.15 | 13.66 | 9.89× | 85.8 | 1976.7 |

The parallel workload needs 72 one-second calls per engine in each round. Four handler slots give an ideal floor of 18 seconds. Both engines approached that limit. Faster orchestration therefore gave a small throughput gain for that case.

The longer chain exposed more coordination cost. The large-output case also exposed serialization and resource costs. These workloads do not measure real network providers, LLM inference, or arbitrary user code.

Throughput varied across rounds:

| Workload | Go workflows/s range | Python workflows/s range |
|---|---:|---:|
| Three-node linear graph | 145.36 to 165.98 | 22.73 to 24.84 |
| Three parallel one-second mocks | 1.32 to 1.32 | 1.27 to 1.29 |
| 50-node chain | 23.35 to 25.87 | 1.78 to 1.80 |
| 192 KiB output | 132.16 to 139.65 | 13.34 to 29.87 |

The large-output Python runs varied substantially. The median ratio needs this context. Three rounds do not supply a strong confidence interval.

**The Go deployment also used less CPU time and sampled memory.**

CPU time sums cumulative Docker CPU counters for all containers in one deployment. Memory is the largest sampled sum of container usage after inactive file cache is subtracted. Sampling can miss short peaks. These values include PostgreSQL and Redis where present.

| Workload | Go CPU seconds | Python CPU seconds | Go sampled MiB | Python sampled MiB |
|---|---:|---:|---:|---:|
| Three-node linear graph | 1.03 | 7.88 | 62.9 | 311.2 |
| Three parallel one-second mocks | 3.48 | 13.37 | 64.6 | 313.3 |
| 50-node chain | 2.99 | 22.93 | 69.9 | 333.1 |
| 192 KiB output | 0.91 | 6.01 | 75.6 | 522.4 |

Each deployment had a total limit of four CPUs and 2 GiB. Each PostgreSQL server received 1.5 CPUs and 768 MiB. The Go application received 2.5 CPUs and 1,280 MiB. Python used 0.5 CPUs for the API, 0.5 for the orchestrator, one for the worker, and 0.5 for Redis.

Python memory limits were 256 MiB for the API, 256 for the orchestrator, 512 for the worker, and 256 for Redis. This allocation compares equal total budgets. Different Python role allocations can change the result.

Both engines used four handler slots, eight slots per execution, a ten-second handler timeout, and a 300-second workflow timeout. API rate limits were 10,000 requests per second. PostgreSQL used synchronous commits. Python Redis used AOF with `appendfsync always`.

| Workload | Workflows per engine per round | Client concurrency | Go overload responses across rounds | Python overload responses across rounds |
|---|---:|---:|---:|---:|
| Three-node linear graph | 128 | 16 | 0 | 58 |
| Three parallel one-second mocks | 24 | 12 | 0 | 0 |
| 50-node chain | 32 | 8 | 0 | 3 |
| 192 KiB output | 64 | 8 | 0 | 0 |

The driver retried `429` and `503` responses after 25 ms. Submission retries reused the same idempotency key. Reported latency and throughput include this retry cost. The result therefore measures completed throughput under controlled overload, not a zero-rejection capacity target.

Across these runs, database audits found 1,488 completed workflows and 13,008 unique persisted tries. Each measured output node used one try. This does not prove exactly-once external effects.

**Both engines recovered accepted work in the process-failure tests.**

Each case ran once per engine. The time includes restart commands and proof that the affected workflow completed. It is not a measurement of HTTP outage duration. The database volume remained intact.

| Failure case | Go seconds to completed workflow | Python seconds to completed workflow | Outcome |
|---|---:|---:|---|
| api sigkill preserves receipt | 1.99 | 4.97 | Both passed |
| control sigkill during handler | 2.51 | 1.83 | Both passed |
| handler process sigkill | 2.52 | 12.96 | Both passed |
| postgres sigkill during handler | 2.86 | 4.44 | Both passed |
| database outage rejects and recovers | 1.72 | 2.75 | Both passed |

The API-failure case saved a receipt before process death and then reused the same key. The same execution returned after restart. The outage case returned `503` while PostgreSQL was unavailable and accepted new work after recovery.

Python kept handler work alive when only its orchestrator stopped. That case needed five tries for the five-node graph. Go restarted the whole application and used eight tries because it repeated three interrupted branch tries. Neither engine repeated the output node.

Python waited for the saved handler lease after worker death. Go classified its interrupted work after it regained exclusive application ownership. This explains part of the recovery-time difference.

Python isolates API, control, and handler process failures. Go shares that application failure boundary and removes the Redis dependency. These are different reliability trade-offs. The tests do not assign a probability to either failure surface.

Power loss, disk destruction, automatic host failover, and prolonged network partitions were outside these process tests. The short benchmark runs also do not establish long-term memory stability. External effects still need provider idempotency or reconciliation.

**The test evidence supports the implementation within this scope.**

The existing Python unit, component, and integration suite passed 573 tests. The Go suite includes database integration tests, race detection, and template-parser fuzz tests. The saved Go coverage reports 78.1% for the engine package and 75.6% across all packages. The executable entry point is outside the native integration coverage.

The Python test image contained cached bytecode with old source paths. The test run used `PYTHONPYCACHEPREFIX=/tmp/fresh-pycache` to compile the current source without changing the Python repository.

Raw evidence:

- [Core benchmark measurements](results/comparison.json)
- [Chain and output measurements](results/comparison_extra.json)
- [Process-failure results](results/reliability.json)
- [Go test log](results/go-tests.log)
- [Go Docker test log](results/go-docker-tests.log)
- [Go fuzz log](results/go-fuzz.log)
- [Python test log](results/python-tests.log)
- [Shared API contract log](results/contract-tests.log)

**PostgreSQL with one Go application is justified for this workload.**

The implementation removes broker transport and several recovery paths while it preserves durable receipts and graph behavior. The measured advantage is strongest when orchestration or serialization dominates the workload. Slow handlers can dominate total runtime and reduce the throughput gain.

The current limits include serialized admission, row contention within one execution, and template work inside claim transactions. Large status pages also need more queries. Those limits need measurements before further architectural changes. No result here establishes that NoSQL is necessary or faster.
