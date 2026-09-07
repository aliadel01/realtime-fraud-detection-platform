# Runbook — Fraud Detection Streaming Pipeline

This runbook explains how to run the full project, step by step.

## Table of Contents
- [Runbook — Fraud Detection Streaming Pipeline](#runbook--fraud-detection-streaming-pipeline)
  - [Table of Contents](#table-of-contents)
  - [1. Start Kafka Cluster \& Producers](#1-start-kafka-cluster--producers)
    - [Services (docker-compose)](#services-docker-compose)
    - [Start the environment](#start-the-environment)
    - [Create topics](#create-topics)


## 1. Start Kafka Cluster & Producers

### Services (docker-compose)

- 3 Redpanda brokers (local multi-broker simulation)
- Redpanda Console (UI for topics, partitions, messages)
- `payment-gateway` producer
- `identity-risk` producer

### Start the environment

Start all services in the background:

```bash
docker compose up -d
```


### Create topics

Create both topics with 6 partitions and 3 replicas:

```bash
rpk topic create transactions.raw --partitions 6 --replicas 3 \
  --brokers redpanda-0:29092,redpanda-1:29093,redpanda-2:29094

rpk topic create identities.raw --partitions 6 --replicas 3 \
  --brokers redpanda-0:29092,redpanda-1:29093,redpanda-2:29094
```

