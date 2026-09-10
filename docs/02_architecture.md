# Architecture Decisions and trade-offs

> This project is built with distributed-processing patterns (parallel consumers, partitioned state, multi-instance Flink task slots) and is designed to scale horizontally. It runs on a single machine / small cluster rather than production-scale infrastructure. Scale-driven mechanisms that can be triggered deliberately at small scale — dynamic repartitioning, consumer lag, backpressure under load, and out-of-memory conditions under resource constraints — are implemented and tested directly, by inducing them on purpose. Concerns that genuinely require production-scale infrastructure or traffic to be real — cross-region replication, cost-driven infrastructure trade-offs at high volume — are reasoned about from principles but not implemented or tested here.

## Table of Contents
- [Architecture Decisions and trade-offs](#architecture-decisions-and-trade-offs)
  - [Table of Contents](#table-of-contents)
  - [Architecture Diagram](#architecture-diagram)
    - [Storage](#storage)
  - [ADRs and Trade-offs](#adrs-and-trade-offs)
    - [ADR-01: Iceberg (not plain data lake or Delta Lake)](#adr-01-iceberg-not-plain-data-lake-or-delta-lake)
    - [ADR-02: Redis for the online feature store](#adr-02-redis-for-the-online-feature-store)
    - [ADR-03: Flink (not Spark Structured Streaming)](#adr-03-flink-not-spark-structured-streaming)
    - [ADR-04: Why using broadcast state for reference data](#adr-04-why-using-broadcast-state-for-reference-data)
    - [ADR-05: Feast Push API not two custom consumers](#adr-05-feast-push-api-not-two-custom-consumers)

## Architecture Diagram
The architecture diagram will be added at the end of the project.

> The architecture logic is complete and ready to be added, but the diagram's visual presentation is not yet finalized.


### Storage

built 3 service containers for storage, each with a specific role in the architecture:
* **`minio`:** Acts as the physical object storage for Iceberg tables, holding both the underlying data files (Parquet) and table metadata (`.metadata.json`, manifest lists, and manifests).
* **`iceberg-catalog`:** The Iceberg REST catalog interface that compute engines like Flink or Spark query to resolve table locations, schema definitions, and snapshot versions.
* **`minio-init`:** An initialization container that automatically provisions the target S3 bucket (`iceberg-warehouse`) on first startup. Without this step, `iceberg-catalog` fails to initialize because its warehouse target path does not exist.




## ADRs and Trade-offs

### ADR-01: Iceberg (not plain data lake or Delta Lake)

**Context:** Raw transactions, historical labeled data, and decisions need atomic writes, schema evolution, and time-travel for audit.

**Decision:** Apache Iceberg on MinIO (lakehouse), not a plain data lake or Delta Lake.

**Rejected alternatives:**
- *Plain files on MinIO* — no ACID guarantees, no schema evolution, no time travel. Directly needed by the idempotency and audit criteria.
- *Delta Lake* — tooling is most mature on Databricks/Spark; this project's engine is Flink. Iceberg is engine-agnostic by design, with first-class Flink support.

**Consequences:** Needs a catalog (e.g. REST/Nessie) as extra infra, in exchange for atomic writes, schema evolution, and snapshots.




### ADR-02: Redis for the online feature store

**Context:** The online feature store needs a single-key lookup — "give user X's rolling velocity/geo features right now" — on every single transaction, inside the $p99 <100ms$ budget (Redis lookup: $<5ms$ of the 100ms total). This is the hottest path in the system.

**Decision:** Redis. All that's needed is a key-value store with sub-millisecond (~1ms) point lookups, and Redis is purpose-built for exactly that access pattern.

**Rejected alternative:** *A traditional DBMS (e.g. Postgres)* — disk-oriented, and a simple `SELECT` by primary key can take 2–5ms, a meaningful slice of the latency budget for the simplest possible query. Not acceptable given the budget. Redis's lack of complex query capability and joins is a non-issue, since none of that is needed here.

**Consequences:** Redis is not durable by default — data can be lost on crash unless persistence is explicitly configured. Accepted, since these feature values are a derived cache, rebuildable from Flink state and Iceberg history, not the source of truth.



### ADR-03: Flink (not Spark Structured Streaming)

**Context:** Needs low latency, precise event-time watermarking, and exactly-once recovery after failure.

**Decision:** Apache Flink.

**Rejected alternative:** *Spark Structured Streaming* — fundamentally micro-batch not per-event processing, even in continuous mode, imposing a latency floor before any real work happens. Flink processes events individually, closer to the sub-100ms requirement.

**Consequences**: Flink's checkpointing model (distributed snapshots) is what provides the exactly-once state recovery this project tests directly (killing a taskmanager mid-stream and verifying correct recovery). The trade-off accepted: Spark has a larger general ecosystem and may be more familiar depending on prior experience — this decision prioritizes architectural fit to the latency and correctness requirements over ecosystem familiarity.

### ADR-04: Why using broadcast state for reference data 
**Context**: Flink needs reference data (user/merchant info) and blacklist data available for every transaction it processes, without adding latency to the hot path — the scoring pipeline runs inside a p99 <100ms budget.

**Decision**: Load reference data into Flink as broadcast state, refreshed periodically, rather than querying Iceberg (or any external store) per-transaction.

**Reasoning**: Iceberg is built for scan access, not single-key point lookups; querying it per-transaction adds I/O latency on the hottest path, for data that only changes daily or occasionally. Broadcast state keeps an in-memory copy local to every Flink task, so the check costs no network call at all.

**Alternative considered**: Per-transaction lookup against Iceberg or a database. Rejected — even a fast database adds a network round-trip on every single transaction, for data whose actual freshness requirement (hours to a day) doesn't justify paying that cost every time.

**Consequences**: Broadcast state must be refreshed on some cadence (e.g. every N minutes) — meaning there's a small window where Flink could act on slightly stale reference data. This is acceptable given the freshness requirement already defined for this data (hours-stale is fine). On restart, broadcast state is rebuilt from Iceberg (the durable source of truth) before processing resumes.

### ADR-05: Feast Push API not two custom consumers

**Context**: Computed features need to reach both the online store (Redis) and the offline store (Iceberg) after Flink produces them. Two architectural options exist: (a) use Feast's Push API, which writes to both stores through one call, or (b) run two independent Kafka consumers, each reading features.stream separately and writing to one store each.

**Decision**: Feast Push API.

**Reasoning**: With two independent consumers, each store's write path can fail or lag independently, letting online and offline drift out of sync with nothing to catch it. Push API gives one write path instead of two separately-failing ones.

**Alternative considered**: Two independent consumers. Rejected — while it does offer full manual control over offsets, retries, and per-store tuning, it means running and monitoring two separate consumer processes instead of one integrated path, and a failure in either one can cause the two stores to disagree with no built-in mechanism to detect it. Feast Push API is simpler operationally and removes an entire class of consistency bugs, at the cost of less granular control over each store's write behavior individually.

**Consequences**: Coupled to the Feast SDK's behavior for both stores — if Feast's push mechanism itself has an outage or bug, both stores are affected together rather than just one. This is accepted as a reasonable trade-off: shared-fate coupling is preferable to silent, undetected divergence between two independently-failing paths.