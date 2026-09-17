# Bronze Ingestion Job — Technical Notes

Dedicated Flink job (Option 2) that reads `transactions.raw` and `identities.raw` and writes each, untransformed, into its own Iceberg table. Runs on its own session cluster (`jobmanager`/`taskmanager`), separate from the feature-computation Flink job — see [ADR-07](02_architecture.md#adr-07-java-not-pyflink-for-the-bronze-ingestion-job) for why it's Java, and `00_technical_challenges.md` / `02_architecture.md` for the surrounding project decisions this builds on.

## Table of Contents
- [Bronze Ingestion Job — Technical Notes](#bronze-ingestion-job--technical-notes)
  - [Table of Contents](#table-of-contents)
  - [Schema](#schema)
  - [Checkpointing = Iceberg commit interval](#checkpointing--iceberg-commit-interval)
  - [Isolation from the feature job](#isolation-from-the-feature-job)
  - [Partitioning](#partitioning)
  - [Known simplification: `event_time`](#known-simplification-event_time)
  - [Idempotent table bootstrap](#idempotent-table-bootstrap)
  - [What this job deliberately does NOT do](#what-this-job-deliberately-does-not-do)
  - [Follow-ups](#follow-ups)

## Schema

Both `bronze.transactions_raw` and `bronze.identities_raw` share the same schema (`IcebergTableBootstrap.BRONZE_SCHEMA`):

| Column | Type | Source |
|---|---|---|
| `kafka_key` | string | `ConsumerRecord.key()` — the (possibly salted) `card1` |
| `kafka_value` | string | `ConsumerRecord.value()` — raw JSON payload, byte-for-byte |
| `kafka_topic` | string | `ConsumerRecord.topic()` |
| `kafka_partition` | int | `ConsumerRecord.partition()` |
| `kafka_offset` | long | `ConsumerRecord.offset()` |
| `event_time` | timestamp | parsed from the `event-time` header |
| `ingest_time` | timestamp | wall-clock time this job processed the record |
| `source_service` | string | `source-service` header (`payment-gateway` / `identity-risk`) |
| `run_id` | string | `run-id` header — ties rows back to a specific producer run |
| `schema_version` | string | `schema-version` header |

`kafka_value` is stored as-is, unparsed. This is the point of a bronze layer: it's a faithful archive of what arrived, not a transformed table. Parsing the transaction/identity JSON into typed columns is a silver-layer job's job, not this one's.

## Checkpointing = Iceberg commit interval

`env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE)` does double duty:

1. It's Flink's standard checkpoint mechanism — snapshotting operator state (including Kafka consumer offsets) every 5 seconds.
2. It's also, transparently, the Iceberg commit interval. The `FlinkSink` writer buffers rows and only calls Iceberg's `commit()` — which creates one new atomic snapshot — when a checkpoint completes.

This is what gives exactly-once end-to-end without writing per-event: Kafka offsets and the Iceberg snapshot advance together, in the same checkpoint. On failure, Flink restores from the last completed checkpoint; any Iceberg data that was buffered but not yet committed for the failed attempt is simply discarded — it was never part of a committed snapshot, so no reader ever saw it, and no offset was advanced past it either. Nothing needs to be rolled back by hand. We can monitor Flink Checkpoint metrics  via the Flink Web UI on `http://localhost:8081/`

## Isolation from the feature job

Each topic gets its own consumer group here (`bronze-ingestion-payment-gateway`, `bronze-ingestion-identity-risk`), distinct from whatever group the feature-computation job uses. Two independent consumer groups reading the same topic means:

- This job's lag, restarts, or redeploys don't affect the feature job's consumption, and vice versa.
- A bug in feature/scoring logic that crashes that job leaves this job's raw archival running untouched — the bronze layer stays the durable, always-on record regardless of what happens upstream in the feature pipeline.

The trade-off, named honestly: both jobs read the same topic independently, so broker read throughput is roughly doubled compared to a single shared-read design (Option 1/4 in the trade-off comparison). Given the topics' local dev throughput, this is a non-issue here; at higher production volume it's a real cost worth re-measuring.

## Partitioning

`PartitionSpec.builderFor(BRONZE_SCHEMA).hour("event_time").build()` — hourly buckets. Chosen over daily (too coarse for a 5-second-checkpoint stream — one partition would accumulate too many files before it's "done") and over no partitioning (every query would scan the whole table). Hourly keeps both compaction (see Follow-ups) and time-range queries ("what came in between 14:00 and 15:00") reasonably cheap.

## Known simplification: `event_time`

The `event-time` header is populated by the producers from `current_dt` — which is `TransactionDT`, a simulation-clock offset in seconds (see `03_source_systems.md`), not a real epoch timestamp. `parseEventTimeHeader()` converts it to millis and stores it as a `TIMESTAMP WITH ZONE` column anyway, for consistency and for downstream ordering/replay logic — but it should not be read as an actual calendar time. This is called out explicitly in the code's Javadoc so it doesn't get silently misinterpreted later.

## Idempotent table bootstrap

Before the streaming pipeline starts, `main()` calls `IcebergTableBootstrap.ensureTable()` synchronously for both tables. It checks `catalog.tableExists()` first — table creation only happens once, on the very first run; every subsequent run (redeploy, restart after failure) is a no-op load. This runs outside the Flink dataflow itself (plain Java, before `env.execute()`), since table DDL is a one-time setup concern, not a per-record streaming operation.

## What this job deliberately does NOT do

- No JSON parsing or schema validation of `kafka_value` — that's silver-layer scope.
- No deduplication logic beyond what exactly-once already provides — `enable.idempotence=True` on the producer side plus this job's exactly-once consumption means duplicates shouldn't reach here in the first place; this job doesn't add a second dedup layer on top.
- No join between the two topics — each topic writes to its own table independently; the transaction↔identity join happens downstream, in the feature-computation job.

## Follow-ups

- **Compaction.** Hourly partitions at a 5s commit interval will still accumulate many small files per partition over a busy hour. A scheduled Iceberg `rewrite_data_files` procedure (run via Spark or Flink batch, on a cron) is needed and is not part of this job — this job's only responsibility is correct, exactly-once ingestion.
