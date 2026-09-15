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
    - [ADR-06: Dedicated Flink Job for Iceberg Bronze Ingestion](#adr-06-dedicated-flink-job-for-iceberg-bronze-ingestion)
    - [ADR-07: Java (not PyFlink) for the bronze-ingestion job](#adr-07-java-not-pyflink-for-the-bronze-ingestion-job)

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


### ADR-06: Dedicated Flink Job for Iceberg Bronze Ingestion

**Context**: Ingesting streaming Kafka topics into Apache Iceberg requires batching to avoid the small-file problem and a Two-Phase Commit mechanism to enforce exactly-once processing. Four patterns were evaluated: single job, dedicated Flink job, Kafka Connect, or side-outputs.

**Decision**: Option 2 — Dedicated Flink job for raw Iceberg ingestion.

**Reasoning**: Decouples raw data storage (Bronze layer) from downstream feature transformations. Isolating raw ingestion into its own job ensures that bugs or backpressure in feature computation logic do not halt raw data persistence into Iceberg.

**Alternatives considered**:

* **Options 1 & 4 (Integrated Sink / Side-Output)**: Rejected due to tight operational coupling. Shared execution graphs cause backpressure or errors in feature code to stall raw data ingestion into Iceberg.
* **Option 3 (Kafka Connect)**: Rejected due to non-atomic offset commits. Kafka Connect decouples table commits from Kafka offset commits; a crash between the two creates duplicates. Flink's checkpointing atomically binds Iceberg snapshot commits to Kafka offsets.

**Consequences**: Adds an extra Kafka consumer group (doubling broker read load for raw topics) and an additional Flink job to monitor. This is an accepted trade-off to guarantee fault isolation and strict exactly-once semantics.

### ADR-07: Java (not PyFlink) for the bronze-ingestion job

**Context:** The bronze-ingestion job (Kafka → Iceberg) needs Flink's `FlinkSink.forRowData()` from the Iceberg-Flink connector, a custom `KafkaRecordDeserializationSchema` that reads partition/offset/headers off the raw `ConsumerRecord`, and RocksDB-backed checkpointing at a 5-second interval. The rest of this project's Python code (`payment_gateway_producer.py`, `identity_risk_producer.py`) exists specifically to use `pandas` for CSV replay and pacing — a justification that does not apply here, since this job does no data-science-style transformation at all.

**Decision:** Write the bronze-ingestion job in Java, using Flink's DataStream API directly.

**Reasoning:**

- **Iceberg's Flink connector is Java-first.** `iceberg-flink-runtime`'s `FlinkSink`, `TableLoader`, and `CatalogLoader` builders are Java/Table-API constructs. PyFlink's DataStream-level support for calling into these builders is thin — the documented, maintained path for Python is the Table/SQL API, not the low-level `RowData` sink builder this job uses to control the exact bronze schema and partition spec.
- **No Python process boundary for pure I/O plumbing.** PyFlink UDFs execute in a separate Python process, communicating with the JVM over the Beam portability layer — a serialization hop on every record. This job does nothing that needs Python's ecosystem (no `pandas`, no `sklearn`); it deserializes bytes, attaches lineage metadata, and writes rows. Paying the cross-process cost here buys nothing.
- **Low-level Kafka record access is more mature in Java.** `KafkaRawRecordDeserializer` reads `record.partition()`, `record.offset()`, and `record.headers()` directly off the `ConsumerRecord` — full access needed for the bronze schema's lineage columns (`kafka_partition`, `kafka_offset`, `source_service`, `run_id`, `schema_version`). PyFlink's Kafka source APIs have historically lagged the Java connector on this kind of record-level access.
- **Debugging stays inside one runtime.** JVM stack traces, thread dumps, and the Flink Web UI's task metrics all point directly at this job's code. A PyFlink job adds a second failure surface (the Python worker process) on top of that, with no corresponding benefit here.
- **Consistency with ADR-03.** Flink was chosen over Spark specifically for low-latency, per-event, JVM-native processing (ADR-03). Introducing a Python execution layer for one of the two Flink jobs in this project undercuts that reasoning for no gain — this job has none of the pandas/CSV-replay needs that justified Python in the producers.

**Rejected alternative:** *PyFlink DataStream API.* Rejected — would require either dropping to Table/SQL API (losing direct control over the bronze `RowData` schema and hourly partition spec) or bridging to the Java `FlinkSink` builder manually via Py4J, an unsupported and fragile path for a production-grade bronze layer. The producers' use of Python is justified by `pandas`; this job has no equivalent justification.

**Consequences:** The bronze-ingestion job is a separate Maven project (own `pom.xml`, own fat jar) rather than reusing the producers' Python container. This is consistent with Option 2's isolation goal anyway — a completely separate deployable, not just a separate Flink job.