# Runbook — Fraud Detection Streaming Pipeline

This runbook explains how to run the full project, step by step.

## Table of Contents
- [Runbook — Fraud Detection Streaming Pipeline](#runbook--fraud-detection-streaming-pipeline)
  - [Table of Contents](#table-of-contents)
  - [1. Start Kafka Cluster \& Producers](#1-start-kafka-cluster--producers)
    - [Services (docker-compose)](#services-docker-compose)
    - [Start the environment](#start-the-environment)
    - [Topics](#topics)

## 1. Start Kafka Cluster & Producers

### Services (docker-compose)

- 3 Redpanda brokers (local multi-broker simulation)
- `topic-init` (creates topics once, then exits)
- Redpanda Console (UI for topics, partitions, messages)
- `payment-gateway` producer
- `identity-risk` producer
- prometheus
- grafana
- hot-key-scanner (on-demand only, via `--profile tools`)
- minio (object storage)
- minio-init (creates the bucket Iceberg writes to)
- iceberg-catalog (Iceberg REST Catalog)
### Start the environment

Start all services in the background:

```bash
docker compose up -d
```

This also creates both topics automatically — `topic-init` runs once at startup and exits (`restart: "no"`), no manual step needed.

### Topics

Both topics are created with **12 partitions** and **3 replicas**:

- `transactions.raw`
- `identities.raw`

To confirm they exist:

```bash
docker compose exec redpanda-0 rpk topic list
```

If you ever need to recreate them manually (e.g. after a full volume wipe):

```bash
docker compose exec redpanda-0 rpk topic create transactions.raw --partitions 12 --replicas 3 \
  --brokers redpanda-0:29092,redpanda-1:29092,redpanda-2:29092

docker compose exec redpanda-0 rpk topic create identities.raw --partitions 12 --replicas 3 \
  --brokers redpanda-0:29092,redpanda-1:29092,redpanda-2:29092
```

