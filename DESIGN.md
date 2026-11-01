**PostgreSQL is the durable authority for one Go application.**

The application contains the API, four bounded handler slots by default, and a recovery loop. PostgreSQL stores workflow facts. A broker and separate worker service add no necessary capability for the built-in mocks.

The data model has related state, unique submission keys, node histories, dependency queries, and atomic output changes. PostgreSQL supplies these directly. NoSQL remains possible, but its name alone does not establish lower latency or better recovery.

**Ownership has two guards.**

One dedicated database connection holds a session advisory lock. The lock name includes the schema. A second application cannot start normally against the same schema.

A row in `engine_control` stores the current owner UUID. Every state transaction reads that row with `FOR SHARE`. Startup changes the UUID before recovery. This guards a previous process that reconnects after PostgreSQL restarts and releases its session lock.

An owner change waits for earlier transactions that still hold their shared guard. Later stale transactions fail the identity comparison. The ownership monitor stops the old application if its dedicated session fails. Docker then restarts the application.

The system permits one active monolith per schema. It does not provide application failover across active hosts. PostgreSQL restart preserves data while its persistent volume remains intact. Host loss needs a separate database availability or backup policy.

**Short execution locks protect concurrent decisions.**

A claim transaction locks one eligible execution with `FOR UPDATE SKIP LOCKED`. It then examines node state, dependencies, retry time, workflow deadline, and the saved concurrency limit. Different executions can claim work concurrently.

The transaction resolves input once and saves it with the new try. The try identity is `(execution_id, node_id, try_number)`. Its record also stores the process owner and handler deadline. The handler starts only after commit.

Handlers do not hold database transactions. Their immutable request contains only the resolved input and mock configuration. A local notification channel reduces scheduler delay. Database scans also find eligible work, so notification loss does not lose work.

A completion transaction takes the ownership guard and execution lock. It compares the current try, node state, process owner, and deadlines. It stores the output, current node state, and final workflow state atomically. Duplicate or stale results cannot replace saved output.

If B and C complete together, their transactions share the execution lock briefly. D becomes eligible only after both saved parent states are `COMPLETED`. A conditional claim and the unique try identity protect its start. No durable broker publication can disappear between parent completion and D's eligibility.

Status and result responses use a repeatable-read snapshot. Each response presents one consistent database view.

**Recovery derives work from saved state.**

Startup examines expired executions and unfinished tries. It marks unfinished tries as interrupted before it schedules their next tries. Saved retry counts, deadlines, frozen inputs, and output budgets remain authoritative.

A committed start consumes a try even if a process stops before the handler call. A finite retry count prevents an endless crash loop for one node. Permanent failure blocks dependent nodes. Unrelated branches can finish.

If a result commit succeeds but its acknowledgment disappears, a repeated completion sees the saved state and makes no second change. If a claim commit has an uncertain outcome, the worker does not start the handler. The next process classifies that saved try as interrupted.

API receipts return after synchronous PostgreSQL commits. A repeated submission key returns the saved execution after a lost response. Trigger acceptance uses the execution lock and accepts one trigger. Clients can read status after a lost trigger response.

The process stops new work when it loses ownership. Transient result-write failures keep the handler slot reserved while the application retries the same outcome. This bounds pending results and prevents a new population of uncommitted handler results.

The maintenance loop selects only due deadlines during normal operation. It uses bounded expiry and retention batches. Startup also selects executions with interrupted tries. It does not scan every active execution on each timer tick.

**Deadlines have a precise acceptance rule.**

PostgreSQL time defines saved deadlines. A handler receives a local timeout from the remaining saved duration. The result transaction accepts success only before the handler deadline and workflow deadline. Late success becomes a timeout or execution expiry.

A database delay can therefore reject a result that existed earlier only in application memory. The Python solution can preserve an on-time result in Redis before later database application. This is a semantic difference at the handler-deadline boundary.

Timeout cancellation works for the cooperative built-in handlers. A timeout does not prove that arbitrary code stopped. The runtime does not accept arbitrary handler code. A future extension needs process isolation or another explicit handler contract.

**Crash recovery does not supply exactly-once external effects.**

The supplied handlers produce mock outputs and make no external requests. A future provider can complete an effect before a crash prevents the local result commit. A later try can repeat that effect.

A stable provider idempotency key needs the execution, node, and logical operation identity. It must remain stable across tries. Without provider support, the engine needs reconciliation, manual resolution, or a policy that permits repeated effects.

**The implementation has known capacity limits.**

Submission admission serializes through one advisory lock and counts saved executions. This preserves exact quotas and provides a direct comparison with the Python solution. It can limit a high submission rate. A separate transactional counter is a possible later change after measurements.

State transitions within one execution serialize through its row. Template resolution currently occurs in the claim transaction. Large fan-in values can increase that lock duration. More handler slots do not remove those costs.

The status endpoint currently reads try histories per returned node. Large status pages need more queries than the Python endpoint's grouped read. This is a measurable cost, not a correctness limit.

The application bounds active handlers, request concurrency, payload sizes, graph size, retries, storage admission, and statement waits. The database still needs enough disk space and suitable backup operations. A single process shares its failure across all application roles.

The comparison report describes measured throughput and observed recovery. A finite test run cannot establish a general failure rate or prove that either engine is always more reliable.
