**This engine runs workflows in one Go application with PostgreSQL.**

The application contains the HTTP API, scheduler, mock handlers, retry logic, and recovery loop. PostgreSQL stores definitions, triggers, frozen inputs, node state, outputs, and try histories. Redis is unnecessary for this design.

The PostgreSQL model fits related state changes and unique identities. NoSQL does not supply a demonstrated performance advantage for this workload. A document or key-value store can support it, but the application still needs atomic transitions and useful query indexes.

**Start the engine with Docker Compose.**

Run commands from this directory. Docker Compose starts one application and one PostgreSQL server.

```sh
docker compose up --build --detach --wait
```

The API uses `http://localhost:29400`. PostgreSQL uses `localhost:29432`. These defaults avoid the existing Python deployment.

Submit and trigger the linear example:

```sh
ID=$(curl -fsS http://localhost:29400/workflow \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: linear-example' \
  --data-binary @examples/linear.json \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["execution_id"])')
curl -fsS -X POST "http://localhost:29400/workflow/trigger/$ID" \
  -H 'Content-Type: application/json' --data '{"input":{"id":7}}'
curl -sS "http://localhost:29400/workflows/$ID"
curl -sS "http://localhost:29400/workflows/$ID/results"
```

The completed result contains `{"outputs":{"c":"mock: ID 7"}}`. For the supplied parallel example, use `examples/parallel.json` and a different submission key. Its three external mocks each wait one second and run concurrently.

Stop the deployment and keep its saved data:

```sh
docker compose down
```

The `--volumes` option deletes saved data.

| Request | Behavior |
|---|---|
| `POST /workflow` | Save a definition and return `201` with `execution_id`. |
| `POST /workflow/trigger/{id}` | Accept one trigger and return `202`. |
| `GET /workflows/{id}` | Return state and paginated node histories. |
| `GET /workflows/{id}/results` | Return `200` after completion or `409` with partial results. |
| `GET /health/live` | Report that the HTTP server can respond. |
| `GET /health/ready` | Examine database access and current process ownership. |

Status queries accept `offset` and `limit`. The default and maximum limit are 100. Submission accepts an optional `Idempotency-Key` with 1 to 128 bytes. A repeated valid key returns the original receipt, even with different valid content. Retention removes the key with its execution. Repeated triggers return `409`.

The trigger supports literal `input` and `node_params` overrides. Template references support fields, array indexes, quoted keys, and escaped braces. Whole-value references preserve JSON types and integer precision. The template parser does not evaluate code. Input data remains literal.

**The application commits work before it starts dependent actions.**

A dedicated PostgreSQL session holds an advisory lock for this database schema. A second application refuses ownership. A saved owner identity also protects writes after database reconnection. Each write transaction examines that identity before it changes workflow state.

Short execution row locks serialize claims, completions, and fan-in decisions within one execution. Different executions can change concurrently. Handlers run outside those transactions. The application stores one start record per try before the handler starts. It stores completion and workflow changes atomically.

The scheduler derives eligible work from saved node state. Its bounded local notification channel only reduces delay. A lost notification cannot lose accepted work. Frozen inputs remain unchanged across retries.

On startup, recovery marks unfinished tries as interrupted and applies their saved retry policy. It applies expired deadlines before dispatch. A crash after a start commit consumes that try, even if the handler never started. Three tries per node are the default.

Loss of the ownership session stops the application. Docker restarts it after PostgreSQL returns. A saved owner identity rejects stale writes from a previous process. A completion also needs the current try, owner, active node state, and valid workflow deadline.

The built-in handlers are trusted, cooperative mocks. They make no external HTTP requests. Cancellation stops their waits, and handler panics become retryable failures. Arbitrary code and native libraries need process isolation before this runtime can safely support them.

A process stop can interrupt work. A durable result remains saved. An unfinished try can run again under its retry policy. Exactly-once external effects are outside this guarantee. A future provider needs a stable idempotency key or an uncertain-outcome policy.

**Resource limits apply before overload can grow without bound.**

| Environment variable | Default | Purpose |
|---|---|---|
| `WF_API_PORT` | `29400` | Published API port in Compose |
| `WF_POSTGRES_PORT` | `29432` | Published PostgreSQL port in Compose |
| `WF_WORKER_CONCURRENCY` | `4` | Maximum active handlers |
| `WF_EXECUTION_SLOTS` | `8` | Maximum active handlers per execution |
| `WF_REQUESTS_PER_SECOND` | `10` | Local API rate limit |
| `WF_MAX_EXECUTIONS` | `100` | Maximum unfinished executions |
| `WF_MAX_RECORDS` | `1000` | Maximum saved executions |
| `WF_HANDLER_TIMEOUT` | `10` | Handler deadline in seconds |
| `WF_EXECUTION_TIMEOUT` | `300` | Workflow deadline in seconds |
| `WF_RETRY_BASE` | `1` | Initial retry delay in seconds |

The graph limit is 500 nodes and 2,000 edges. The request limit is 1 MiB. Ordinary node values use a 256 KiB limit. Aggregate inputs and outputs use separate 8 MiB budgets. Output handlers can use the aggregate value limit.

The engine also bounds request concurrency, connection acquisition, statements, retry counts, and retention batches. It returns `429` for rate limits and `503` for admission or storage failures. Completed outputs need durable storage. New acceptance stops when PostgreSQL cannot commit.

**Run the automated tests with Docker or Go 1.27.**

The Docker test profile includes race detection and PostgreSQL integration tests:

```sh
docker compose -p workflow-go-check --profile test run --build --rm test
```

For local Go tools, use these commands.

Start an isolated database before integration tests. This command uses its own project and volume.

```sh
docker compose -p workflow-go-check up --detach --wait postgres
WF_TEST_POSTGRES_DSN='postgres://workflow:workflow@localhost:29432/workflow?sslmode=disable' \
  go test -race -count=1 -coverprofile=coverage.out -v ./...
go vet ./...
go test ./internal/engine -run '^$' -fuzz FuzzTemplateParser -fuzztime=5s
```

Without `WF_TEST_POSTGRES_DSN`, the Go tests skip database cases. Integration tests create separate schemas and remove them afterward. Do not run the isolated database command while the normal deployment occupies its port.

The suites cover graph errors, templates, bounds, large integers, concurrent submissions, one trigger, parallel branches, and fan-in. They also cover duplicate results, stale owners, retries, expiry, retention, and restart recovery.

**Compare both complete deployments with the same workload.**

The comparison uses a separate Compose project, ports, and volumes. It never controls the existing Python deployment. Create the source images first:

```sh
docker compose build app
docker build -t workflow-python-compare:local --target runtime \
  -f ../event-driven-workflow-orchestration-engine/infra/Dockerfile \
  ../event-driven-workflow-orchestration-engine
python3 -m venv .venv
.venv/bin/pip install -r scripts/requirements.txt
.venv/bin/python scripts/compare.py all --rounds 3
```

To run the original Python suite against these isolated dependencies, use this command after the comparison creates them:

```sh
docker build -t workflow-python-test:local --target test \
  -f ../event-driven-workflow-orchestration-engine/infra/Dockerfile \
  ../event-driven-workflow-orchestration-engine
docker run --rm --network workflow-go-compare_default \
  -e PYTHONPYCACHEPREFIX=/tmp/fresh-pycache \
  -e WF_POSTGRES_DSN=postgresql://workflow:workflow@pg-python:5432/workflow \
  -e WF_REDIS_URL=redis://redis:6379/0 \
  -e WF_REQUIRE_INFRA=1 -e WF_ENABLE_TEST_FIXTURES=1 \
  workflow-python-test:local pytest -q tests/unit tests/component tests/integration
```

The fresh bytecode directory avoids cached source paths from the source image.

The driver uses `workflow-go-compare` and its dedicated containers. It stops those application containers between engine runs. Fault cases kill those application or database containers. The database volumes remain intact.

Each deployment has a total limit of four CPUs and 2 GiB of memory. Each PostgreSQL server receives 1.5 CPUs and 768 MiB. Both engines use four handler slots, equal deadlines, and equal graph data. PostgreSQL uses synchronous commits. Python Redis uses `appendfsync always`.

The Go application receives the budget that Python spends on its API, orchestrator, worker, and Redis. This compares complete architectures. It does not isolate the language change.

The driver records throughput, workflow latency, receipt latency, CPU time, sampled memory, overload responses, and persisted try identities. It retries `429` and `503` after 25 ms with the same submission key. Reported latency includes those retries. It alternates run order. The mock workload cannot establish performance for real providers.

The fault suite covers a lost API process, a lost control process, a lost handler process, PostgreSQL process death, and a database outage. A Go process failure affects all application roles. Python isolates those roles in separate processes.

The workload set includes linear graphs, parallel mocks, 50-node chains, and 192 KiB outputs. Results appear in `results/comparison.json`, `results/comparison_extra.json`, and `results/reliability.json`.

Run the shared public contract after the comparison completes:

```sh
.venv/bin/python -m pytest -q scripts/test_contract.py
```

This command uses the same isolated project and changes which engine runs. [COMPARISON.md](COMPARISON.md) describes the measured outcome and its limits. [DESIGN.md](DESIGN.md) records the implemented safety rules.

Remove only the comparison deployment after a run:

```sh
docker compose -p workflow-go-compare -f scripts/compare.compose.yml down
```
