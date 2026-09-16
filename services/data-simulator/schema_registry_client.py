"""
Shared Schema Registry + Avro serializer factory for producers.

One function, one job: load a .avsc file, register it against the
Registry (redpanda's built-in one, already up at :18081), hand back
a ready-to-use AvroSerializer.

Registration is idempotent — Schema Registry checks the new schema
against the subject's compatibility mode (topic-init already sets
BACKWARD globally) and either:
  - accepts it silently if it's the same schema, or
  - accepts it if it's backward-compatible (new optional fields,
    widened types), or
  - REJECTS the produce call with an error if it's a breaking change
    (removed field, tightened type, renamed field).
That rejection at produce-time is the entire point: a breaking change
gets caught the moment a producer restarts with a bad schema, not
three hops downstream when the Flink job throws a deserialization
exception on record #40,000.
"""
import os
import json
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import Schema

SCHEMA_REGISTRY_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://redpanda-0:18081")


def load_schema_str(avsc_path: str) -> str:
    with open(avsc_path) as f:
        return json.dumps(json.load(f))


def build_avro_value_serializer(avsc_path: str, topic: str = "transactions.raw", to_dict_fn=None) -> tuple:
    """
    to_dict_fn(obj, ctx) -> dict, matching the schema fields.
    Default: obj is already a plain dict, pass through as-is.
    returns (schema_version, AvroSerializer)
    """
    
    registry = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    schema_str = load_schema_str(avsc_path)
    
    serializer = AvroSerializer(
        registry,
        schema_str,
        to_dict=to_dict_fn if to_dict_fn else (lambda obj, ctx: obj),
    )
    
    avro_schema = Schema(schema_str, "AVRO")
    subject_name = f"{topic}-value"
    
    schema_id = registry.register_schema(subject_name, avro_schema)
    
    registered_schema = registry.get_version(subject_name, schema_id)
    schema_version = registered_schema.version

    return schema_version, serializer


def build_key_serializer() -> StringSerializer:
    """
    Build a StringSerializer for keys.
    keys stay plain strings (card1 / TransactionID) — no schema needed,
    partitioning/salting logic (hot_key_utils.build_key) is unaffected.
    """
    return StringSerializer("utf_8")