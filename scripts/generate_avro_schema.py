"""
Generate .avsc schema from a CSV header.

Why generate, not hand-write: train_transaction.csv has 390+ columns
(C1-C14, D1-D15, M1-M9, V1-V339...). Hand-writing = error-prone, breaks
on any upstream column change. This script is the single source of
truth for "what does the schema look like today" — run it once per
dataset version, review the diff, commit the .avsc, register it.

Rules baked in here (all support safe schema evolution later):
  - every field except the declared REQUIRED ones is a UNION with null
    and carries "default": null  -> new consumers can add optional
    fields later without breaking old readers (Avro BACKWARD compat).
  - REQUIRED fields (TransactionID / TransactionDT, or TransactionID
    for identity) have no default -> they must always be present,
    that's the join/ordering/partition key contract.
  - numeric-looking columns get ["null","double"], everything else
    ["null","string"] — CSV doesn't reliably tell you int vs float
    (NaNs coerce ints to float in pandas anyway), so double is the
    safe universal numeric type.
"""
import sys
import json
import pandas as pd

REQUIRED_FIELDS = {
    "transaction": ["TransactionID", "TransactionDT"],
    "identity": ["TransactionID"],
}

# columns whose semantic type must NOT be left to dtype inference —
# TransactionID is a join/partition key, must always be long even
# though a NaN-heavy sample can coerce pandas int columns to float64.
TYPE_OVERRIDES = {
    "TransactionID": "long",
}


def infer_avro_type(col_name: str, series: pd.Series) -> str:
    if col_name in TYPE_OVERRIDES:
        return TYPE_OVERRIDES[col_name]
    if pd.api.types.is_numeric_dtype(series):
        return "double"
    return "string"


def build_schema(csv_path: str, record_name: str, namespace: str) -> dict:
    df = pd.read_csv(csv_path, nrows=5000)  # sample is enough to infer dtypes
    required = REQUIRED_FIELDS.get(record_name, [])

    fields = []
    for col in df.columns:
        avro_type = infer_avro_type(col, df[col])
        if col in required:
            fields.append({"name": col, "type": avro_type})
        else:
            fields.append({
                "name": col,
                "type": ["null", avro_type],
                "default": None,
            })

    return {
        "type": "record",
        "name": record_name,
        "namespace": namespace,
        "fields": fields,
    }


if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("usage: generate_avro_schema.py <csv_path> <record_name> <namespace> <out.avsc>")
        sys.exit(1)

    csv_path, record_name, namespace, out_path = sys.argv[1:5]
    schema = build_schema(csv_path, record_name, namespace)

    with open(out_path, "w") as f:
        json.dump(schema, f, indent=2)

    print(f"wrote {out_path} ({len(schema['fields'])} fields)")
