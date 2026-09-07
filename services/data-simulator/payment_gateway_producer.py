"""
payment-gateway producer.

Read train_transaction.csv, replay rows to Kafka topic transactions.raw,
sorted by TransactionDT, timed at SPEED_FACTOR speedup (1hr real -> 1sec sim).

Key: card1 (fallback TransactionID if card1 missing) — group txns per card,
preserve per-card order in same partition.

Durability: acks=all + enable.idempotence=True + max.in.flight<=5 — no lost,
no duplicate, ordered writes even on retry.

Headers attach lineage: source-service, run-id (per script run), schema-version,
event-time — trace/debug without touching payload schema.

Env-independent config: TRANSACTIONS_PATH, KAFKA_BROKER hardcoded here for
local sim (redpanda 3-broker cluster). Swap to os.environ for prod deploy.

Known gap: no schema registry yet (raw JSON, bronze layer). No hot-key
salting yet (add build_key() + hot_keys.json once monitoring detect skew).
"""

import os
import json
import time
import logging
import pandas as pd
import uuid
from confluent_kafka import Producer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("payment-gateway")

TRANSACTIONS_PATH = os.environ["TRANSACTIONS_PATH"]
KAFKA_BROKER = os.environ["KAFKA_BROKER"]
SPEED_FACTOR = 3600
TOPIC = "transactions.raw"
MAX_SLEEP = float("2.0")
RUN_ID = str(uuid.uuid4())

producer_conf = {
    "bootstrap.servers": KAFKA_BROKER,
    "linger.ms": 5,
    "compression.type": "snappy",
    "acks": "all",
    "enable.idempotence": True,
    "retries": 2147483647,           # let delivery.timeout.ms control retry budget, not this
    "request.timeout.ms": 30000,     # wait per single request
    "delivery.timeout.ms": 120000,   # total budget: queue -> success/fail, incl retries
    "max.in.flight.requests.per.connection": 5,
    "batch.size": 32768,
    "client.id": "payment-gateway",  
}


def delivery_report(err, msg):
    if err is not None:
        log.error(f"delivery failed: {err}")


def row_to_json(row):
    return json.dumps(row.where(pd.notnull(row), None).to_dict())


def run():
    log.info("loading transactions...")
    tx = pd.read_csv(TRANSACTIONS_PATH)
    tx = tx.sort_values("TransactionDT").reset_index(drop=True)

    producer = Producer(producer_conf)
    prev_dt = None
    total = len(tx)
    log.info(f"payment-gateway starting: {total} txns, speed_factor={SPEED_FACTOR}")

    for i, row in tx.iterrows():
        current_dt = row["TransactionDT"]
        log.debug(f"txn {i}: TransactionDT={current_dt}, card1={row['card1']}")
        if prev_dt is not None:
            delta = (current_dt - prev_dt) / SPEED_FACTOR
            delta = max(0.0, min(delta, MAX_SLEEP))
            if delta > 0:
                time.sleep(delta)
        prev_dt = current_dt

        card_key = str(row["card1"]) if pd.notnull(row["card1"]) else str(row["TransactionID"])
        
        producer.produce(
            TOPIC,
            key=card_key,
            value=row_to_json(row),
            headers=[
                ("source-service", b"payment-gateway"),      
                ("run-id", RUN_ID.encode("utf-8")),
                ("schema-version", b"v1"),
                ("event-time", str(current_dt).encode("utf-8")),
            ],
            callback=delivery_report,
        )
        producer.poll(0)

        if i % 5000 == 0:
            log.info(f"progress: {i}/{total}")

    log.info("payment-gateway replay done, flushing...")
    producer.flush(30)
    log.info("payment-gateway closed clean")


if __name__ == "__main__":
    run()