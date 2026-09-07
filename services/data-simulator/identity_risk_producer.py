"""
identity-risk producer.

Read train_identity.csv, join (left) with light transaction ref cols
(TransactionID, TransactionDT, card1) from train_transaction.csv — join
used ONLY for pacing + partition key, never fabricate identity content.

Replay to Kafka topic identities.raw, sorted by TransactionDT (nulls last).
Orphan rows (no matching transaction) = real source noise, kept as-is,
fallback key = TransactionID when card1 missing.

Simulates real imperfect source system: identity event may arrive
before/after/without matching transaction event — this async gap is
intentional, not a bug. Downstream Flink job must handle via windowed
stream-stream join with watermark + allowed lateness, not fixed here.

Durability: acks=all + enable.idempotence=True + max.in.flight<=5.

Headers attach lineage: source-service, run-id, schema-version, event-time.

Known gap: no schema registry yet. No hot-key salting yet (card1 skew
same risk as payment-gateway — apply same build_key() fix when ready).
"""
import os
import json
import time
import logging
import pandas as pd
import uuid
from confluent_kafka import Producer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("identity-risk")

IDENTITIES_PATH = os.environ["IDENTITIES_PATH"]
TRANSACTIONS_PATH = os.environ["TRANSACTIONS_PATH"]
KAFKA_BROKER = os.environ["KAFKA_BROKER"]
SPEED_FACTOR = 3600
TOPIC = "identities.raw"
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
    "client.id": "identity-risk", 
}


def delivery_report(err, msg):
    if err is not None:
        log.error(f"delivery failed: {err}")


def row_to_json(row):
    return json.dumps(row.where(pd.notnull(row), None).to_dict())


def run():
    log.info("loading identities...")
    idn = pd.read_csv(IDENTITIES_PATH)

    log.info("loading transaction timing/key ref (light cols only)...")
    tx_ref = pd.read_csv(
        TRANSACTIONS_PATH, usecols=["TransactionID", "TransactionDT", "card1"]
    )

    # join for pacing + key only, not fabricating identity content
    merged = idn.merge(tx_ref, on="TransactionID", how="left")
    # rows with no match = orphan identity events, real source noise, keep as-is
    merged = merged.sort_values("TransactionDT", na_position="last").reset_index(drop=True)

    producer = Producer(producer_conf)
    prev_dt = None
    total = len(merged)
    log.info(f"identity-risk starting: {total} events, speed_factor={SPEED_FACTOR}")

    for i, row in merged.iterrows():
        current_dt = row["TransactionDT"]

        if prev_dt is not None and pd.notnull(current_dt):
            delta = (current_dt - prev_dt) / SPEED_FACTOR
            delta = max(0.0, min(delta, MAX_SLEEP))
            if delta > 0:
                time.sleep(delta)
        if pd.notnull(current_dt):
            prev_dt = current_dt

        # orphan record: no card1 match, use TransactionID as fallback key
        card_key = str(row["card1"]) if pd.notnull(row["card1"]) else str(row["TransactionID"])

        # drop join-only helper cols before sending, keep payload true to source
        payload_row = row.drop(labels=["TransactionDT", "card1"])
        producer.produce(
            TOPIC,
            key=card_key,
            value=row_to_json(payload_row),
            headers=[
                ("source-service", b"identity-risk"),
                ("run-id", RUN_ID.encode("utf-8")),
                ("schema-version", b"v1"),
                ("event-time", str(current_dt).encode("utf-8")),
            ],
            callback=delivery_report,
        )
        producer.poll(0)

        if i % 5000 == 0:
            log.info(f"progress: {i}/{total}")

    log.info("identity-risk replay done, flushing...")
    producer.flush(30)
    log.info("identity-risk closed clean")


if __name__ == "__main__":
    run()