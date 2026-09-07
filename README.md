# Real-Time Fraud Detection Platform

A streaming system that scores transactions for fraud in real time, built to prove specific **engineering guarantees** — not just to demonstrate that a pipeline runs.

The system receives a continuous stream of transactions. For each one, it must decide `allow`, `flag`, or `block` within a strict latency budget, using both the transaction itself and the user's recent behavioral history. This decision must remain correct even when part of the system fails or restarts mid-stream.

The decision is made by a machine learning model trained on highly imbalanced data — fraud is rare compared to legitimate transactions — so the model is built to handle that **imbalance** deliberately, rather than optimizing for accuracy alone.

**Throughput and latency** are treated as one combined requirement, not two separate ones: a design that hits the latency budget only under low load doesn't solve the problem.

You can read more about problem definition, source systems, decision model & storage in the [01_Problem_Definition](docs/01_problem_definition.md) document.

> [!IMPORTANT] Note
> This project isn't a set of streaming tools wired together. It's built around simulating the engineering problems a production real-time system actually faces — **late and out-of-order data**, **failures and restarts**, **feature staleness**, and **imbalanced data** — and solving each one deliberately, with evidence. A personal project can't fully reproduce production conditions, but this one is built to get close enough that these problems are real and testable, not assumed away.

## Success Criteria

### 1. Systems Criteria

**Latency**: **p99 <100ms** per transaction, including all processing and model scoring, measured under the maximum sustained throughput the deployed infrastructure can handle (see Throughput below). Average latency reported as a secondary metric, not the primary target.

**Throughput**: **No fixed target number** set in advance. Instead: measure the maximum sustained throughput the deployed infrastructure can handle for **10 minutes** without growing consumer lag, and report that measured number honestly, alongside the hardware/cluster spec it was measured on.

**Exactly-once / recovery**: **Zero duplicate decisions** in the `fraud_decisions` table after an induced Flink taskmanager kill mid-stream. Processing resumes correctly within **60 seconds** of the failure, with **no data loss**.

**Late/out-of-order data**: Feature values remain correct (matching expected value, verified by manual calculation) when a subset of events are injected with up to **30 seconds** of artificial delay.

**Feature freshness**: Online store (Feast/Redis) lag behind event-time is measured under load. A maximum acceptable lag is defined (e.g. 5 seconds) based on how quickly a geo-jump or velocity pattern could become stale; any measured lag beyond that threshold is flagged as a defect, not just reported.

**Schema evolution**: A new field added to the Avro schema does not break any existing consumer or the Flink job, verified by an explicit before/after compatibility test — not just asserted from configuration.

**Backpressure / degradation**: Under 2x normal producer load, consumer lag is allowed to grow but must recover to baseline within **60 seconds** once load returns to normal, with zero dropped or duplicated events.

**Idempotent writes**: Replaying the same Kafka offset range twice produces **zero** duplicate rows in Iceberg, verified by row count and primary key check.

### 2. ML Criteria

**Discrimination performance**: F1 ≥ 0.8 on a held-out test set, with precision and recall both ≥ 0.75.

**Operating threshold**: The classification threshold is chosen against the precision/recall curve using an explicitly stated cost assumption (e.g. cost of a missed fraud case vs. cost of a false block), not left at the default 0.5 — and that assumption is documented, not just the resulting threshold.

**Imbalance handling**: The chosen imbalance strategy (class weighting or resampling — pick one, justify against the other) is compared against a naive baseline (no imbalance handling) on the same test set, with both results reported, so the improvement is evidence-backed, not assumed.

**Robustness**: F1 score does not degrade by more than 5% (relative) when evaluated on a test set with injected noise and randomly dropped features, compared to the clean test set.

**Training/serving consistency**: A sample of live-scored transactions is periodically compared against offline batch scoring on the same events, to confirm the online feature path produces the same values as the training path (catches skew introduced by pipeline bugs, not just staleness).

> [!WARNING] Remember
> Define what actually passes the success criteria after finishing the project [Success Criteria](docs/success_criteria.md) document. 


## Architecture