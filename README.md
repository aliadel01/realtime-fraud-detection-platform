# Real-Time Fraud Detection Pipeline

A synchronous, streaming fraud-detection system: a transaction event comes in, and a classifier decision (`allow` / `flag` / `block`) has to go out inside a **p99 < 100ms** budget, backed by a full audit trail of the state each decision was based on. Source systems (payment gateway, CRM, merchant management, security watchlist) are simulated in Python rather than connected to real external systems — see `01_problem_definition.md` for the full input/output/storage model.

The engineering interest of this project isn't the fraud model itself (it's a baseline XGBoost, deliberately not tuned — see `06_ml_service.md`). It's the data engineering underneath it: exactly-once ingestion, schema evolution across independently-deployed producers, hot-key handling on a skewed real-world key, and hitting a hard latency budget without sacrificing correctness.

## Success Criteria



**Latency**: **p99 <100ms** per transaction, including all processing and model scoring, measured under the maximum sustained throughput the deployed infrastructure can handle (see Throughput below). Average latency reported as a secondary metric, not the primary target.



**Throughput**: **No fixed target number** set in advance. Instead: measure the maximum sustained throughput the deployed infrastructure can handle for **10 minutes** without growing consumer lag, and report that measured number honestly, alongside the hardware/cluster spec it was measured on.



**Exactly-once / recovery**: **Zero duplicate decisions** in the `fraud_decisions` table after an induced Flink taskmanager kill mid-stream. Processing resumes correctly within **60 seconds** of the failure, with **no data loss**.



**Late/out-of-order data**: Feature values remain correct (matching expected value, verified by manual calculation) when a subset of events are injected with up to **30 seconds** of artificial delay.



**Feature freshness**: Online store (Feast/Redis) lag behind event-time is measured under load. A maximum acceptable lag is defined (e.g. 5 seconds) based on how quickly a geo-jump or velocity pattern could become stale; any measured lag beyond that threshold is flagged as a defect, not just reported.



**Schema evolution**: A new field added to the Avro schema does not break any existing consumer or the Flink job, verified by an explicit before/after compatibility test — not just asserted from configuration.



**Backpressure / degradation**: Under 2x normal producer load, consumer lag is allowed to grow but must recover to baseline within **60 seconds** once load returns to normal, with zero dropped or duplicated events.



**Idempotent writes**: Replaying the same Kafka offset range twice produces **zero** duplicate rows in Iceberg, verified by row count and primary key check.

## Documentation Map

| File | Covers |
|---|---|
| `01_problem_definition.md` | Source systems, decision model, storage requirements, freshness targets |
| `02_architecture.md` | Project-wide architecture decisions (ADR-01 → ADR-05, ADR-07) — Iceberg, Redis, Flink, broadcast state, Feast, Java |
| `03_source_systems.md` | The two Kafka producers — config choices, hot-key handling (ADR-08), schema-evolution enforcement (ADR-09) |
| `04_ingestion_job.md` | The bronze ingestion job — why it's a separate job (ADR-06), partitioning (ADR-10), exactly-once mechanism |
| `05_processing_layer.md` | The feature-computation Flink job (rolling windows, broadcast joins) |
| `06_ml_service.md` | The model and feature set, ONNX serving |
| `07_cross_cutting_concerns.md` | Problems that span more than one stage — idempotency, latency/fault-tolerance, watermarking, delivery semantics, observability, schema evolution end-to-end |

Every design decision with real alternatives considered is numbered (`ADR-01` through `ADR-10` so far) and lives in whichever file the decision is local to — not in one flat list. `02_architecture.md` holds the ones that apply project-wide; the stage files hold the ones local to that stage.

## Core Competencies

Short version of every hard problem this project actually had to solve, with a link to the full reasoning — not re-explained here.

| Problem | Approach | Details |
|---|---|---|
| **Hot key risk on a skewed real-world key** | `card1` as partition key (ordering requirement), confirm-gated reactive salting, static-partition escalation for chronic cases | [ADR-08](03_source_systems.md#adr-08-hot-key-risk-and-data-skew) |
| **Schema evolution across independent deploys** | Avro + Schema Registry, BACKWARD compatibility, rejected at produce time — not three hops downstream | [ADR-09](03_source_systems.md#adr-09-schema-evolution--producer-enforcement), [bronze side](04_ingestion_job.md#schema-evolution--the-bronze-side) |
| **Exactly-once Kafka → Iceberg** | One checkpoint interval drives both the Kafka offset commit and the Iceberg snapshot commit — atomically, without a separate 2PC layer | [ADR-06 / Mechanism](04_ingestion_job.md#mechanism-exactly-once-via-checkpoint--commit) |
| **p99 < 100ms decision latency** | Redis for sub-5ms point lookups, broadcast state to avoid per-transaction I/O, Flink for per-event (not micro-batch) processing | [Low-Latency & Fault-Tolerant Delivery](07_cross_cutting_concerns.md#low-latency--fault-tolerant-delivery) |
| **Safe reruns everywhere (idempotency)** | Three different mechanisms at three different layers — broker-level producer dedup, idempotent table bootstrap, checkpoint-atomic commits | [Idempotent Pipeline Design](07_cross_cutting_concerns.md#idempotent-pipeline-design-safe-reruns) |
| **Out-of-order data, two different causes** | Bounded-out-of-orderness watermarks reconstruct order both after intentional hot-key salting and across two independently-paced producers | [Watermarking & Out-of-Order Data](07_cross_cutting_concerns.md#watermarking--out-of-order-data) |
| **Delivery semantics end-to-end** | At-least-once from the source, made effectively-once by the producer, made exactly-once into Iceberg — two separately-guaranteed hops, not one unified mechanism | [Delivery Semantics](07_cross_cutting_concerns.md#delivery-semantics-exactly-once-vs-at-least-once--deduplication) |
| **Observability** | Native Prometheus partition-skew alerting, Flink checkpoint health — freshness SLA is defined but **not yet wired to a real alert**, called out honestly rather than hidden | [Monitoring and Observability](07_cross_cutting_concerns.md#monitoring-and-observability) |