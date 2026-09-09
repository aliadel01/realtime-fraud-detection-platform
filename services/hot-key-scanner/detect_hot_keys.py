"""
detect_hot_keys.py — manual, on-call-triggered hot key management.

Two modes:

  detect  — run when a Grafana partition-skew alert fires. Scans ONE
            flagged partition. Finds keys that are individually heavy
            right now. Use to decide whether to START salting a key.

  demote  — run on a schedule (e.g. daily cron), independent of alerts.
            Scans the WHOLE topic, strips salt suffixes, and sums each
            raw key's share back together across all its salted
            buckets/partitions. Use to decide whether to STOP salting
            a key that has cooled down. Must be topic-wide: a still-hot
            salted key is invisible to a single-partition scan because
            salting spreads it thin across N partitions.

Both modes only ever PRINT a proposed change and wait for interactive
confirmation. Neither mode writes hot_keys.json without a "y".

Sampling window — time-based, not a fixed message count:

  A fixed message count (e.g. "last 50,000 messages") represents a
  different amount of WALL-CLOCK time depending on how fast traffic is
  flowing at the moment you scan — during a burst it might be 5
  minutes, during quiet traffic it might be several days. That makes
  the "is this key hot RIGHT NOW" question inconsistent.

  Instead, both modes seek to a starting offset found via
  offsets_for_times() for "--lookback-minutes minutes ago" — a
  constant, comparable time window every time you scan, regardless of
  traffic rate.

  --sample-size is kept as an UPPER CAP, not the primary control: it
  bounds how many messages a single run will read, so a sudden traffic
  spike inside the lookback window can't make the scan run for an
  unbounded amount of time. Whichever limit is hit first — the time
  window or the message cap — stops the read.

If detect mode finds zero keys above threshold in a flagged partition,
that is itself the signal: the heaviness is likely Murmur2 hash
collision among several medium-volume keys, not a single hot key.
Salting will not fix that case — it needs a partition-count review
instead.
"""
import os
import re
import json
import time
import uuid
import logging
import argparse
from collections import Counter
from confluent_kafka import Consumer, TopicPartition

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("hot-key-scanner")

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "redpanda-0:29092,redpanda-1:29092,redpanda-2:29092")
HOT_KEYS_PATH = os.environ.get("HOT_KEYS_PATH", "/app/data/hot_keys.json")
HEAVY_THRESHOLD = float(os.environ.get("HEAVY_THRESHOLD", "0.02"))  # >2% of scanned traffic

# Defaults — both overridable per-run via --lookback-minutes / --sample-size
DEFAULT_LOOKBACK_MINUTES = int(os.environ.get("LOOKBACK_MINUTES", "10"))
DEFAULT_SAMPLE_SIZE_CAP = int(os.environ.get("SAMPLE_SIZE", "50000"))

POLL_TIMEOUT_SEC = 1.0
MAX_EMPTY_POLLS = 30
SEEK_RETRY_ATTEMPTS = 30
SEEK_RETRY_WAIT_SEC = 0.5

SALT_SUFFIX_RE = re.compile(r"-\d+$")


def strip_salt(key: str) -> str:
    return SALT_SUFFIX_RE.sub("", key)


def load_hot_keys() -> set:
    if os.path.exists(HOT_KEYS_PATH):
        with open(HOT_KEYS_PATH) as f:
            return set(json.load(f))
    return set()


def write_hot_keys(keys: set):
    os.makedirs(os.path.dirname(HOT_KEYS_PATH), exist_ok=True)
    with open(HOT_KEYS_PATH, "w") as f:
        json.dump(sorted(keys), f)


def confirm(prompt: str) -> bool:
    return input(f"{prompt} [y/N] ").strip().lower() == "y"


def seek_with_retry(consumer: Consumer, target: TopicPartition):
    """assign() settles asynchronously inside librdkafka — seek() can fail
    with 'Erroneous state' if called before that settles. Retry with a
    generous budget instead of a single attempt."""
    last_err = None
    for _ in range(SEEK_RETRY_ATTEMPTS):
        try:
            consumer.seek(target)
            return
        except Exception as e:
            last_err = e
            consumer.poll(SEEK_RETRY_WAIT_SEC)
    raise RuntimeError(f"seek() never succeeded after retries: {last_err}")


def resolve_time_based_start(consumer: Consumer, topic: str, partition: int, lookback_minutes: int):
    """Return (start_offset, high_watermark) for `lookback_minutes` ago on
    this partition, falling back to the low watermark if there's no data
    that far back (offsets_for_times returns offset=-1 in that case)."""
    ts_ms = int(time.time() * 1000) - (lookback_minutes * 60 * 1000)
    tp = TopicPartition(topic, partition, ts_ms)
    result = consumer.offsets_for_times([tp], timeout=10)
    offset = result[0].offset

    low, high = consumer.get_watermark_offsets(TopicPartition(topic, partition), timeout=10)
    if offset is None or offset < 0:
        offset = low  # no message old enough / partition empty that far back
    return max(low, offset), high


# ---------------------------------------------------------------------
# detect mode — single flagged partition, targeted, alert-triggered
# ---------------------------------------------------------------------

def scan_partition(topic: str, partition: int, lookback_minutes: int, sample_size_cap: int) -> Counter:
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": f"hot-key-scanner-detect-{uuid.uuid4()}",  # unique, no offset creep
        "enable.auto.commit": False,
    })
    tp = TopicPartition(topic, partition)
    consumer.assign([tp])

    start, high = resolve_time_based_start(consumer, topic, partition, lookback_minutes)
    # sample_size_cap still applies: even if the time window contains more
    # messages than the cap, we only read the most recent `sample_size_cap`
    # of them, so a traffic spike can't make this run unbounded.
    start = max(start, high - sample_size_cap)

    seek_with_retry(consumer, TopicPartition(topic, partition, start))

    counts = Counter()
    scanned = 0
    empty_polls = 0
    try:
        while scanned < sample_size_cap and empty_polls < MAX_EMPTY_POLLS:
            msg = consumer.poll(POLL_TIMEOUT_SEC)
            if msg is None or msg.error():
                empty_polls += 1
                continue
            empty_polls = 0
            scanned += 1
            raw_key = strip_salt(msg.key().decode())
            counts[raw_key] += 1
    finally:
        consumer.close()

    return counts


def run_detect(topic: str, partition: int, lookback_minutes: int, sample_size_cap: int):
    log.info(
        f"scanning {topic} partition {partition} "
        f"(last {lookback_minutes} min, capped at {sample_size_cap} msgs)..."
    )
    counts = scan_partition(topic, partition, lookback_minutes, sample_size_cap)

    if not counts:
        log.info("no messages sampled, nothing to do")
        return

    total = sum(counts.values())
    heavy = {k for k, c in counts.items() if c / total > HEAVY_THRESHOLD}

    if not heavy:
        print(
            f"\nNo key above {HEAVY_THRESHOLD:.0%} threshold found in partition {partition} "
            f"({total} msgs sampled).\n"
            "This partition's heaviness is likely Murmur2 hash collision among several "
            "medium-volume keys, not a single dominant key. Salting will not fix this — "
            "escalate for a partition-count review instead.\n"
        )
        return

    old_hot_keys = load_hot_keys()
    added = heavy - old_hot_keys

    print(f"\npartition {partition}: {len(heavy)} heavy key(s) found ({total} msgs sampled)")
    for k in sorted(heavy):
        share = counts[k] / total
        status = "NEW" if k in added else "already tracked"
        print(f"  {k}: {share:.1%} of partition traffic  [{status}]")

    if not added:
        print("\nAll found heavy keys are already in hot_keys.json. Nothing to add.")
        return

    print(f"\nProposed addition: {sorted(added)}")
    if confirm("Apply this update to hot_keys.json?"):
        new_hot_keys = old_hot_keys | added  # union — never drop other partitions' keys
        write_hot_keys(new_hot_keys)
        log.info(f"hot_keys.json updated: {len(old_hot_keys)} -> {len(new_hot_keys)}")
    else:
        log.info("aborted, no change written")


# ---------------------------------------------------------------------
# demote mode — whole topic, scheduled, checks if tracked keys cooled
# ---------------------------------------------------------------------

def scan_topic_for_demotion(topic: str, lookback_minutes: int, sample_size_cap: int) -> Counter:
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": f"hot-key-scanner-demote-{uuid.uuid4()}",
        "enable.auto.commit": False,
    })

    metadata = consumer.list_topics(topic, timeout=10)
    partitions = list(metadata.topics[topic].partitions.keys())
    per_partition_cap = max(sample_size_cap // max(len(partitions), 1), 1)

    targets = []
    for p in partitions:
        start, high = resolve_time_based_start(consumer, topic, p, lookback_minutes)
        start = max(start, high - per_partition_cap)
        targets.append(TopicPartition(topic, p, start))

    consumer.assign(targets)
    for tp in targets:
        try:
            seek_with_retry(consumer, tp)
        except RuntimeError as e:
            log.warning(f"seek never settled for partition {tp.partition}, skipping it: {e}")

    counts = Counter()
    scanned = 0
    empty_polls = 0
    try:
        while scanned < sample_size_cap and empty_polls < MAX_EMPTY_POLLS:
            msg = consumer.poll(POLL_TIMEOUT_SEC)
            if msg is None or msg.error():
                empty_polls += 1
                continue
            empty_polls = 0
            scanned += 1
            raw_key = strip_salt(msg.key().decode())  # recombine salted buckets
            counts[raw_key] += 1
    finally:
        consumer.close()

    return counts


def run_demote(topic: str, lookback_minutes: int, sample_size_cap: int):
    log.info(
        f"scanning {topic} topic-wide for demotion check "
        f"(last {lookback_minutes} min, capped at {sample_size_cap} msgs)..."
    )
    old_hot_keys = load_hot_keys()

    if not old_hot_keys:
        log.info("hot_keys.json is empty, nothing to demote")
        return

    counts = scan_topic_for_demotion(topic, lookback_minutes, sample_size_cap)
    if not counts:
        log.info("no messages sampled, skipping (keeping current hot_keys.json unchanged)")
        return

    total = sum(counts.values())
    cooled = set()
    for k in sorted(old_hot_keys):
        share = counts.get(k, 0) / total
        if share < HEAVY_THRESHOLD:
            cooled.add(k)
        log.info(f"  {k}: {share:.2%} of topic traffic (threshold {HEAVY_THRESHOLD:.0%})")

    if not cooled:
        print("\nNo tracked hot keys have cooled down. hot_keys.json unchanged.")
        return

    print(f"\nCandidates for removal (below {HEAVY_THRESHOLD:.0%} for this scan): {sorted(cooled)}")
    if confirm("Remove these from hot_keys.json?"):
        new_hot_keys = old_hot_keys - cooled
        write_hot_keys(new_hot_keys)
        log.info(f"hot_keys.json updated: {len(old_hot_keys)} -> {len(new_hot_keys)}")
    else:
        log.info("aborted, no change written")


# ---------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["detect", "demote"], required=True)
    parser.add_argument("--topic", required=True)
    parser.add_argument("--partition", type=int, help="required for --mode detect")
    parser.add_argument(
        "--lookback-minutes", type=int, default=DEFAULT_LOOKBACK_MINUTES,
        help=f"how far back to look, in minutes (default: {DEFAULT_LOOKBACK_MINUTES})"
    )
    parser.add_argument(
        "--sample-size", type=int, default=DEFAULT_SAMPLE_SIZE_CAP,
        help=f"upper cap on messages read, regardless of lookback window (default: {DEFAULT_SAMPLE_SIZE_CAP})"
    )
    args = parser.parse_args()

    if args.mode == "detect":
        if args.partition is None:
            parser.error("--partition is required for --mode detect")
        run_detect(args.topic, args.partition, args.lookback_minutes, args.sample_size)
    else:
        run_demote(args.topic, args.lookback_minutes, args.sample_size)


if __name__ == "__main__":
    main()