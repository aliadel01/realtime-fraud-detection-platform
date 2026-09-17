# Source Systems

## Table of Contents
- [Source Systems](#source-systems)
  - [Table of Contents](#table-of-contents)
  - [Transaction events producer (simulate payment gateway) and Identity risk producer](#transaction-events-producer-simulate-payment-gateway-and-identity-risk-producer)
    - [Configuration Choices](#configuration-choices)
    - [Partition count + local multi-broker simulation](#partition-count--local-multi-broker-simulation)
  - [Hot Key Risk and Data Skew](#hot-key-risk-and-data-skew)
    - [When Does This Happen?](#when-does-this-happen)
    - [Solution A: Partition Key and Baseline Count](#solution-a-partition-key-and-baseline-count)
    - [Solution B: Detection — Partition-Level Monitoring](#solution-b-detection--partition-level-monitoring)
    - [Solution C: Reactive Fix — Confirm-Gated Salting](#solution-c-reactive-fix--confirm-gated-salting)
    - [Solution D: Escalation — Dedicated Partition](#solution-d-escalation--dedicated-partition)
    - [Producer-Side Hot Key Reload](#producer-side-hot-key-reload)
    - [Evidence](#evidence)
  - [Schema Evolution — Producer Enforcement](#schema-evolution--producer-enforcement)
    - [Decision](#decision)
    - [Rejected Alternatives](#rejected-alternatives)
    - [Rule](#rule)
    - [Failure Mode: Rejected at Produce Time](#failure-mode-rejected-at-produce-time)
    - [Operational Handling (Not Yet Implemented)](#operational-handling-not-yet-implemented)
  - [Data Restructuring: Transaction, User Reference, and Merchant Data](#data-restructuring-transaction-user-reference-and-merchant-data)
    - [Why We Split the Data](#why-we-split-the-data)
    - [Current Version: Keep It Simple](#current-version-keep-it-simple)
    - [Next Step: Orchestration and Real-Time Updates](#next-step-orchestration-and-real-time-updates)

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

$$\text{partitions} = \max\left(\frac{\text{target throughput}}{\text{per partition producer throughput}}, \frac{\text{target throughput}}{\text{per partition consumer throughput}}\right)$$

For local testing, Redpanda supports a 3-node cluster through docker-compose:
`KAFKA_BROKER = "redpanda-0:29092,redpanda-1:29093,redpanda-2:29094"`

Why 12 partitions specifically, and how that number relates to hot-key risk, is **ADR-08** immediately below — this section is the mechanical config, not the reasoning.

> [!NOTE]
> Since transaction and identity events come from two separate producers, small delays (around 5ms) between them are fine — even good. They mimic real production behavior, and give the Flink job a real join problem to solve (see `07_cross_cutting_concerns.md#watermarking--out-of-order-data` for how that join is actually handled).

## Hot Key Risk and Data Skew

Hot key risk happens when a small number of partition-key values get much more traffic than the others, pushing all events for that key into a single Kafka partition. Kafka's parallelism depends on partitions, so a hot key creates a hard limit on speed no matter how big the cluster is — the affected partition falls behind while the rest sit idle, causing lag, delayed fraud scoring, and possible SLA problems.

The partition key here is `card1`, so this risk is structural, not incidental, from two directions at once: a legitimate cardholder can genuinely burst (a corporate card running through a Black Friday sale), and a fraud pattern can look identical from the partitioning system's point of view (a card-testing script hammering the same `card1` to validate stolen numbers). Both land on the same partition, for the same mechanical reason — and the second case is exactly the traffic this pipeline exists to catch, arriving on the one partition most likely to be lagging.

### When Does This Happen?

| Cause | Example | Nature |
|---|---|---|
| Single dominant key | Corporate fleet cards, payment bots, card-testing scripts on `card1` | One key genuinely overloads a partition |
| Hash collision | `Murmur2(key) mod N` maps several medium-volume keys to the same partition | No single key is "hot" — the mapping is |
| Low-cardinality key | `country_code`, `payment_status` | Structurally guarantees collision at any volume |

The distinction matters operationally: salting fixes the first case and does nothing for the second.

### Solution A: Partition Key and Baseline Count

We chose `card1` as the partition key. Fraud detection requires per-card ordering — this single requirement eliminates every lower-cardinality alternative (`country_code`, `payment_status`) outright, independent of their skew profile.

We chose a baseline partition count of **12** to mitigate hash-collision skew specifically — more buckets lowers the probability Murmur2 maps several medium-volume keys into the same partition. This does **not** fix a single dominant hot key; it only spreads baseline key entropy. 12 is a development baseline, and scales with `target_throughput / per_partition_throughput` in production.

### Solution B: Detection — Partition-Level Monitoring

We monitor per-partition size using Redpanda's native Prometheus metrics endpoint (`:9644`, `vectorized_storage_log_partition_size`) — no `kafka_exporter` sidecar needed, unlike stock Kafka, which requires one for equivalent visibility.

```promql
max by (topic) (vectorized_storage_log_partition_size)
/
avg by (topic) (vectorized_storage_log_partition_size)
> 1.5
```
Sustained 5 minutes before alerting. This check works at the partition level, not the key level — cheap, always-on, and catches the problem regardless of cause (dominant key or hash collision).

![Heavy Partition Dashboard](./images/heavy_partition_dashboard.png)

### Solution C: Reactive Fix — Confirm-Gated Salting

`detect_hot_keys.py` is the tool this project built to answer "is this key actually hot, and should we salt it": it samples real traffic from Kafka, decides which keys cross a heaviness threshold, and only writes an update to `hot_keys.json` after a human confirms it.

We chose to confirm before salting, not salt automatically, because a wrong salt has a real cost — it breaks the order of an innocent key, and that cost is high enough to justify a human in the loop on every write, in both directions (adding and removing).

We also chose to sample by **time window**, not a fixed message count. A fixed count like "last 50,000 messages" represents a different amount of wall-clock time depending on traffic rate — minutes during a burst, days during quiet traffic — making "is this key hot right now" inconsistent. Instead, both modes resolve a start offset via `offsets_for_times()` for `--lookback-minutes` minutes ago, giving a comparable window regardless of traffic rate. `--sample-size` is kept only as an upper cap, so a burst inside the window can't make the scan run unbounded — whichever limit hits first stops the read.

**Mode 1 — `detect` (alert-triggered, single partition)**
```
python detect_hot_keys.py --mode detect --topic X --partition N \
  --lookback-minutes 10 --sample-size 50000
```
1. On-call runs this with the partition ID from the alert.
2. Script targets only that partition (`assign()`), seeks to the start of the lookback window (capped by `--sample-size`).
3. Counts key share within the sampled window.
4. No key crosses `HEAVY_THRESHOLD` (2%) → prints "likely hash collision, not hot-key." Exits, no write.
5. Diffs found heavy keys against `hot_keys.json` — `added = heavy - old`.
6. `added` empty → "nothing to add." Exits, no write.
7. `added` non-empty → prints the list, prompts `Apply update? [y/N]`.
8. `y` → merges into the existing set (union — never wipes other partitions' keys), writes the file. Anything else → aborts, no write, decision logged.

**Mode 2 — `demote` (scheduled cron, topic-wide)**
```
python detect_hot_keys.py --mode demote --topic X \
  --lookback-minutes 60 --sample-size 50000
```
1. Runs on a schedule, independent of alerts.
2. `hot_keys.json` empty → exits immediately.
3. Discovers all partitions, seeks each to the lookback window (cap split evenly), samples the whole topic.
4. Every key passed through `strip_salt()` before counting — recombines a salted key's traffic across all its buckets, since a single-partition scan would only see `1/SALT_BUCKETS` of its real share.
5. Each tracked key whose summed share is now `< HEAVY_THRESHOLD` → candidate for removal.
6. No keys cooled down → "nothing to remove." Exits, no write.
7. Otherwise prints candidates, prompts `Remove these? [y/N]`. `y` → removes and writes. Anything else → aborts, no write.

> [!NOTE]
> Salting intentionally breaks per-partition physical order for a hot key. Consumers must key-by the raw (unsalted) `card1` and use a bounded-out-of-orderness event-time window in Flink to reconstruct true per-card order before fraud sequence detection runs (see `07_cross_cutting_concerns.md#watermarking--out-of-order-data`). The watermark bound is a latency/correctness trade-off sized from observed lag, not assumed.
>
> If `detect` finds no key above threshold in a flagged partition, that's a hash-collision signal, not a hot key — salting will not help; escalate for a partition-count review instead.

> See the producer-side reload mechanism (how a confirmed update reaches a running producer without a restart) in [Producer-Side Hot Key Reload](#producer-side-hot-key-reload) below.

### Solution D: Escalation — Dedicated Partition

If a hot key is persistent and structural (e.g., a massive corporate account), we isolate it to a dedicated partition using a custom static `Partitioner` — instead of leaving it salted indefinitely. This preserves strict message ordering without resorting to salting, at the cost of needing dedicated consumer capacity for that one partition so it doesn't lag under its own isolated load.

> [!IMPORTANT]
> Partition count = the baseline, always active. Salting = the default reactive fix. Dedicated partition = only used when the same key stays hot across two or more scans — a chronic problem, not a short burst.

**Production cost, named honestly:** partition count is fixed at deploy time — raising it later means resharding, which this design avoids by over-provisioning now (12, cheap) rather than reacting later (reshard, expensive). Dedicated-partition escalation doesn't auto-scale either — each one is a standing operational commitment (capacity reserved for that partition), not a one-time fix.

### Producer-Side Hot Key Reload

This section covers how producers pick up hot-key updates at runtime — the mechanism, not the decision (that's Solution C above).

**End-to-End Flow**

![Producer-side hot key reload flow](images/hot_key_detection_flow.png)

**What This Solves**

Once on-call confirms a hot key update via `detect_hot_keys.py`, the change is written to `hot_keys.json` on disk. Producers need to pick up this change **without a restart** and **without wastefully polling the disk** when nothing has changed.

**1. Salting logic — `build_key()`**

`hot_key_utils.py` exposes a single function every producer calls in place of using the raw key directly:

```python
def build_key(raw_key: str) -> str:
    if raw_key in _store.get():
        bucket = random.randint(0, SALT_BUCKETS - 1)
        return f"{raw_key}-{bucket}"
    return raw_key
```

If the raw key (`card1`) is **not** in the current hot-key set, it passes through unchanged — this is the common case, and it's what preserves per-card ordering for every normal card, which the downstream fraud sequence detection depends on. Only a key confirmed hot gets a `-{bucket}` suffix appended, where `bucket` is chosen uniformly at random from `0` to `SALT_BUCKETS - 1`. This spreads that one key's traffic across `SALT_BUCKETS` sub-partitions instead of one, at the cost of breaking that key's own ordering for the duration it stays salted.

**2. Reload strategy — event-driven, not blind polling**

`HotKeyStore` is held once in producer memory (RAM) and backs `build_key()`'s lookups. The reload is triggered by a file change, not a fixed timer:

1. Every `MTIME_CHECK_INTERVAL_SEC` (default 5s), the store checks the file's **modification time** via `os.path.getmtime()` — a cheap OS `stat` call that does not open or read the file.
2. If the mtime is unchanged since the last check, the store returns immediately. **No file read happens.**
3. Only when the mtime has actually changed (meaning `detect_hot_keys.py` wrote a confirmed update) does the store open the file and `json.load()` it into memory.

This means: no confirmed change on disk → stat only, ever, no read. A confirmed change (on-call approves via `detect_hot_keys.py`) → picked up within `MTIME_CHECK_INTERVAL_SEC`, no producer restart.

**3. Race-condition handling**

`detect_hot_keys.py` writes `hot_keys.json` with a plain `open(...).write()` — not atomic. If a producer's mtime check happens to land mid-write, `json.load()` can raise `json.JSONDecodeError` on a partially-written file. `HotKeyStore._reload()` catches this (and `OSError`) silently and keeps the previous in-memory key set until the next check succeeds:

```python
except (json.JSONDecodeError, OSError):
    pass  # mid-write, keep old set
```

This avoids needing file locking between the writer (`detect_hot_keys.py`, run manually and infrequently) and the readers (producers, checking every few seconds) — the cost of a missed read is just one extra `MTIME_CHECK_INTERVAL_SEC` of delay before the update lands, not a crash.

**4. Configuration**

| Env var | Default | Meaning |
|---|---|---|
| `HOT_KEYS_PATH` | `/app/data/hot_keys.json` | Must be the same path used by `detect_hot_keys.py` — a mismatch means the producer silently never sees updates. |
| `SALT_BUCKETS` | `4` | Number of sub-partitions a hot key is spread across once salted. |
| `MTIME_CHECK_INTERVAL_SEC` | `5` | How often the cheap `stat` check runs. Lower = faster pickup, more syscalls; higher = the reverse. 5s balances both without needing tuning. |

### Evidence

**`detect` — alert on partition 0:**
```
$ docker compose --profile tools run --rm hot-key-scanner \
    python detect_hot_keys.py --mode detect --topic transactions.raw \
    --partition 0 --lookback-minutes 10 --sample-size 50000

partition 0: 12 heavy key(s) found (447 msgs sampled)
  10086: 2.0% of partition traffic  [NEW]
  10486: 2.5% of partition traffic  [already tracked]
  ...
Proposed addition: ['10086', '10960', '14649', '1556', '15757', '16630', '16709', '17966', '9002', '9350']
Apply this update to hot_keys.json? [y/N] y
hot_keys.json updated: 9 -> 19
```
![](./images/salting_example1.png)
![](./images/salting_example2.png)

**`demote` — deliberately undersized sample (500) to force a demotion, confirming the removal path end-to-end:**
```
$ docker compose --profile tools run --rm hot-key-scanner \
    python detect_hot_keys.py --mode demote --topic transactions.raw \
    --lookback-minutes 10 --sample-size 500

  10086: 0.20% of topic traffic (threshold 2%)
  10486: 0.61% of topic traffic (threshold 2%)
  ...
Candidates for removal (below 2% for this scan): ['10086', '10486', '10960', ...]
Remove these from hot_keys.json? [y/N] y
hot_keys.json updated: 19 -> 0
```
At `--sample-size 500`, every key needs 10/500 messages to clear 2% — a demanding bar, which is why every tracked key demoted in this run. Not a realistic production sample size (tens of thousands, as in the `detect` example) — used here only to exercise the confirm-gate and removal path.

## Schema Evolution — Producer Enforcement

Producers and consumers in this pipeline deploy independently. `payment-gateway` and `identity-risk` can ship a schema change on their own cadence; the bronze job, the future feature-computation job, and any offline retraining job keep running against data written weeks or months earlier. Every Flink job in this project reads from `OffsetsInitializer.earliest()` — full-topic replay is a first-class requirement (see ADR-01, `04_ingestion_job.md`), not an edge case.

### Decision

Avro schemas (`transaction_v1.avsc`, `identity_v1.avsc`) registered against a Schema Registry, compatibility mode **BACKWARD**, enforced at produce time via `AvroSerializer` (`schema_registry_client.py`).

### Rejected Alternatives

- **FORWARD** — protects an old consumer reading new data. Not our failure mode: this pipeline doesn't run frozen consumer code against evolving data; it replays old data with new code.
- **FULL** — BACKWARD + FORWARD. Blocks safe additive changes (new field + default) to buy a guarantee (FORWARD) nothing here depends on.
- **NONE** — removes the only safety net the pipeline has. Defeats the stated purpose of `schema_registry_client.py`: catch a breaking schema at the producer, not three hops downstream in a Flink deserialization exception.

### Rule

New schema must read data written by the previous schema:

| Change | Allowed | Condition |
|---|---|---|
| Add field | ✅ | must declare `default` |
| Remove field | ✅ | always |
| Widen type | ✅ | only Avro promotion path (`int→long→float→double`, `string↔bytes`) |
| Narrow type | ❌ | no promotion path in that direction |
| Rename field | ❌ | unless `"aliases"` points to the old name |
| Reorder fields | ✅ | Avro resolves by name, not position |

### Failure Mode: Rejected at Produce Time

| Change | Example | Rejected because |
|---|---|---|
| Required field, no default | `riskScore: double` | old data has no value, no default to fill it |
| Rename, no alias | `card1 → cardId` | resolves as delete + required-add |
| Union narrowed | `isFraud: ["null","double"] → "double"` | old data allowed `null` |
| Unsupported type change | `TransactionID: long → string` | no safe conversion |
| Type demotion | `TransactionAmt: double → float` | promotion is one-directional |
| Record renamed, no alias | `transaction → TransactionEvent` | reader can't match old versions |

> "That rejection at produce-time is the entire point: a breaking change gets caught the moment a producer restarts with a bad schema, not three hops downstream when the Flink job throws a deserialization exception on record #40,000." — `schema_registry_client.py`

Fail-fast means zero bad records ever reach Kafka. The alternative — catching it downstream — means finding and deleting records already committed into Iceberg. (What happens on the *consuming* side of this — bronze deliberately not enforcing any reader schema — is covered in `04_ingestion_job.md#schema-evolution--the-bronze-side`.)

### Operational Handling (Not Yet Implemented)

Current state: a rejected schema raises inside `producer.produce()`, uncaught, crashes the replay script.

Required for production:
- Catch the serializer's error specifically, not a bare `except`.
- Log CRITICAL, not retry — this is a code/deploy defect, not a transient broker issue.
- Stop the producer. A visible gap beats a silent, dropped fraud event.
- Move the check left: run the Registry's `/compatibility` endpoint against every `.avsc` change in CI, before the container ships.

## Data Restructuring: Transaction, User Reference, and Merchant Data

### Why We Split the Data

The original dataset (`train_transaction.csv`) comes from Kaggle as one flat file. All transaction data, user information, and merchant information are mixed together in the same table.

This is not how a real production fraud detection system looks. In a real system (see [01_Problem_Definition](01_problem_definition.md#source-systems)), transaction events, user reference data, and merchant data come from **different source systems**:

- **Transaction events** come from the payment gateway, in real time.
- **User reference data** (card info, address, email) comes from a CRM system, updated daily.
- **Merchant data** (merchant risk score) comes from a merchant management system, also updated daily.

These systems are separate in real life. They have different owners, different update speeds, and different infrastructure. If we keep everything in one flat table, we lose this structure, and we cannot simulate the real architecture our project is built around (see [ADR-04](02_architecture.md#adr-04-why-using-broadcast-state-for-reference-data)).

So we split the flat Kaggle data into three parts, to simulate three separate source systems:

1. **`train_transaction_fact.csv`** — the transaction stream. Contains only fast-changing, per-event data (amount, product code, engineered V/C/D features, identity/device info).
2. **`user_reference.csv`** — a dimension table with slow-changing user/card attributes (card type, address, email domain).
3. **`merchant_data.csv`** — a dimension table with merchant risk information (fraud rate per product category).

Each transaction record has a foreign key (`user_reference_sk`, `merchant_data_sk`) pointing to the correct row in each dimension table. This is the same join pattern the real system will use: Flink reads the transaction stream and joins it with reference data through broadcast state, instead of one large merged table.

### Current Version: Keep It Simple

Right now, `user_reference.csv` and `merchant_data.csv` each store **only the latest known state** for every user (`card1`) or merchant category (`ProductCD`). There is no history and no versioning yet.

We chose this simple version first because:
- It is enough to build and test the first working pipeline end-to-end (data split → features → model → ONNX export).
- It avoids extra complexity before the rest of the system (Flink, Feast, broadcast state) is working.
- It matches the project's step-by-step approach: get something simple running correctly first, then improve it.

### Next Step: Orchestration and Real-Time Updates

Later, we will build a separate **orchestration process** that keeps `user_reference` and `merchant_data` up to date automatically, instead of computing them once from a static file.

This orchestration will:
- Detect real changes in the source data (for example, a card's address changes, or a merchant's fraud rate shifts).
- Update the reference tables to reflect the current state, similar in spirit to how [`detect_hot_keys.py`](03_source_systems.md#adr-08-hot-key-risk-and-data-skew) already updates `hot_keys.json` on a schedule.
- Keep each reference table close to real time, so that when a transaction arrives, Flink's broadcast state always reflects the most current known user and merchant state — not an outdated snapshot.

At that point, we will also add proper history tracking (SCD Type 2: `valid_from`, `valid_to`, `is_current`) so we can reconstruct what a user's or merchant's state was at any past point in time — useful for audit and for training data correctness. For now, we keep it simple: current state only, no history.