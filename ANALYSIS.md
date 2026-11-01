**The selected implementation uses PostgreSQL with one Go monolith.**

This decision replaces the earlier SQLite proposal. The Python solution is already submitted. The Go implementation provides a separate architecture for comparison and discussion.

PostgreSQL fits the task because workflow acceptance, node state, try histories, and dependent work share atomic boundaries. Unique keys protect receipt and try identities. SQL also supplies dependency and status queries.

NoSQL is possible, but it does not remove these requirements. A key-value design needs its own indexes and atomic transition model. A document design needs a rule for concurrent changes and output growth. No evidence establishes a NoSQL performance advantage for this workload.

The monolith removes Redis transport, separate worker processes, result notification repair, and the dispatch outbox. It stores enough state to reconstruct work after a process crash. A local notification channel is temporary. Durable workflow state remains authoritative.

The application uses PostgreSQL ownership and execution locks because its API and handler goroutines can make concurrent state decisions. One advisory session lock prevents a second active application. A saved process owner protects writes after PostgreSQL restarts. Short row locks serialize fan-in and claims for one execution.

A pure in-memory engine remains valid when restart data loss is acceptable. That is a different failure contract. This implementation returns successful receipts only after durable commits and keeps interrupted work for recovery.

The durability guarantee assumes an intact persistent database volume and correct storage synchronization. Application or PostgreSQL process death does not delete that volume. Disk destruction, host failover, and exactly-once external effects need separate policies.

The main capacity limits are transaction rate, execution lock duration, payload size, and available handler slots. The current admission path also serializes exact quotas and counts saved records. These are concrete targets for later measurement and optimization.

[DESIGN.md](DESIGN.md) describes the implemented state and recovery rules. [README.md](README.md) gives setup and test commands. [COMPARISON.md](COMPARISON.md) reports the actual Go and Python measurements.
