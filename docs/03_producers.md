# Source Systems

## Table of Contents
- [Source Systems](#source-systems)
  - [Table of Contents](#table-of-contents)
  - [Transaction events producer (simulate payment gateway) and Identity risk producer](#transaction-events-producer-simulate-payment-gateway-and-identity-risk-producer)
    - [Configuration Choices](#configuration-choices)
    - [Partition count + local multi-broker simulation](#partition-count--local-multi-broker-simulation)
  - [Summary](#summary)

## Transaction events producer (simulate payment gateway) and Identity risk producer

### Configuration Choices
**1. Linger.ms**\
We set `"linger.ms": 5` to allow a little batching, but not too much. This is a trade-off between latency and throughput.

Compared to a higher value like 20ms: more small packets, less compression benefit, higher CPU and network cost per message. But fraud detection needs fast detection. So we choose a low `linger.ms` (1-5ms). The overhead cost is small. It is worth it for fresh, real-time data.

**2. Prevents both duplicates and out-of-order messages.**\
`"enable.idempotence": True` because we cannot allow duplicate messages. This choice can increase latency: if a retry happens, the next events wait until that retry finishes. But this is good, because it keeps order correct. It prevents both duplicates and out-of-order messages. Idempotence forces `acks=all` (you need full replication confirmation for the guarantee to work) and requires `retries > 0` (idempotence exists to make retries safe).

`"acks": "all"` — the end-to-end latency stays the same, but the response time (how long `send()` takes to confirm) becomes longer. We use an asynchronous producer, so this is not a problem. The only limit: if there are 5 in-flight requests, the producer waits and sends no more until one of the 5 gets acknowledged (`"max.in.flight.requests.per.connection": 5`).

**3. Send pattern: async + callback**\
We send messages asynchronously with a callback pattern: `delivery_report()` logs errors when a send fails, `poll(0)` drains callbacks on every loop, and `flush(30)` waits for everything left in the buffer before the script ends.

**4. Timeouts**
$$\text{delivery.timeout.ms} \ge \text{request.timeout.ms} + \text{linger.ms}$$

- `request.timeout.ms = 30000` (30 sec — wait time for one single broker reply)
- `delivery.timeout.ms = 120000` (2 min — total budget, including all retries)
- `retries`: set to a high number. We let `delivery.timeout.ms` control when to stop, not the retry count itself.

**5. Compression**\
`compression.type = snappy` gives low CPU cost. A 5ms linger time is too short for a strong compression ratio, but snappy is still better than no compression at all.

**6. Headers**\
We add headers to each message to help with debugging and tracking: `source-service`, `run-id`, `schema-version`, and `event-time` (event-time comes from `current_dt` in the merged row). Headers allow fast message routing and filtering based on metadata, without extra CPU cost — a consumer can read the header directly, without deserializing the full message value.

### Partition count + local multi-broker simulation

Formula:
$$\text{partitions} = \max\left(\frac{\text{target\_throughput}}{\text{per\_partition\_producer\_throughput}}, \frac{\text{target\_throughput}}{\text{per\_partition\_consumer\_throughput}}\right)$$

At dev/sim scale, we don't have a real load test yet. So we simply pick 6 partitions per topic, based on the expected max parallel consumers (Flink task parallelism).

Adding partitions later breaks key-to-partition stability. Dynamic resharding can cause out-of-order events, so we avoid that in production. This is why we pick a number high enough now, instead of raising it later.

For local testing, Redpanda supports a 3-node cluster through docker-compose:
`KAFKA_BROKER = "redpanda-0:29092,redpanda-1:29093,redpanda-2:29094"`

> [!NOTE]
> Since transaction and identity events come from two separate producers, small delays (around 5ms) between them are fine — even good. They mimic real production behavior, and give the Flink job a real join problem to solve.

## Summary