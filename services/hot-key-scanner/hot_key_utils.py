"""
Shared hot-key salting logic for producers.

Loads hot_keys.json (written by detect_hot_keys.py) and spreads only
known heavy keys across SALT_BUCKETS sub-partitions. Normal keys pass
through unchanged, keeping per-card ordering intact for the fraud
sequence detection that depends on it.

Reload strategy: event-driven, not blind polling. Every
MTIME_CHECK_INTERVAL_SEC we do a cheap os.path.getmtime() stat call
(no file read). Only when the mtime has actually changed do we open
and json.load() the file. This means:
  - No confirmed change on disk -> stat only, no read, ever.
  - A confirmed change (on-call approves via detect_hot_keys.py)
    -> picked up within MTIME_CHECK_INTERVAL_SEC, no producer restart.
"""
import os
import json
import time
import random

HOT_KEYS_PATH = os.environ.get("HOT_KEYS_PATH", "/app/data/hot_keys.json")
SALT_BUCKETS = int(os.environ.get("SALT_BUCKETS", "4"))
MTIME_CHECK_INTERVAL_SEC = float(os.environ.get("MTIME_CHECK_INTERVAL_SEC", "5"))


class HotKeyStore:
    def __init__(self):
        self._keys = set()
        self._mtime = 0.0
        self._last_check = 0.0
        self._reload()

    def _reload(self):
        try:
            mtime = os.path.getmtime(HOT_KEYS_PATH)
        except OSError:
            return  # file doesn't exist yet, keep current (empty) set

        if mtime == self._mtime:
            return  # file unchanged since last read, skip the read entirely

        try:
            with open(HOT_KEYS_PATH) as f:
                self._keys = set(json.load(f))
            self._mtime = mtime
        except (json.JSONDecodeError, OSError):
            pass  # file mid-write or unreadable, keep previous keys

    def get(self) -> set:
        now = time.monotonic()
        if now - self._last_check >= MTIME_CHECK_INTERVAL_SEC:
            self._last_check = now
            self._reload()
        return self._keys


_store = HotKeyStore()


def build_key(raw_key: str) -> str:
    if raw_key in _store.get():
        bucket = random.randint(0, SALT_BUCKETS - 1)
        return f"{raw_key}-{bucket}"
    return raw_key