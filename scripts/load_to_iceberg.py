"""
Load ref CSVs (user_reference, merchant_data) into Iceberg via REST catalog.
Run on host machine. Docker compose stack must be up.

pip install pyiceberg[s3fs,pyarrow] pandas
"""

import pandas as pd
import pyarrow as pa
from pyiceberg.catalog import load_catalog

# --- config ---
CATALOG_URI = "http://localhost:8181"
S3_ENDPOINT = "http://localhost:9000"
S3_ACCESS_KEY = "admin"
S3_SECRET_KEY = "password"
WAREHOUSE = "s3://iceberg-warehouse/"

NAMESPACE = "bronze"

FILES = {
    "user_reference": r"data\tests\user_reference.csv",
    "merchant_data": r"data\tests\merchant_data.csv",
}


def get_catalog():
    return load_catalog(
        "rest",
        **{
            "uri": CATALOG_URI,
            "warehouse": WAREHOUSE,
            "s3.endpoint": S3_ENDPOINT,
            "s3.access-key-id": S3_ACCESS_KEY,
            "s3.secret-access-key": S3_SECRET_KEY,
            "s3.path-style-access": "true",
        },
    )


def ensure_namespace(catalog, namespace):
    if namespace not in [ns[0] for ns in catalog.list_namespaces()]:
        catalog.create_namespace(namespace)


def load_csv_to_iceberg(catalog, table_name, csv_path):
    df = pd.read_csv(csv_path)
    arrow_table = pa.Table.from_pandas(df, preserve_index=False)

    full_name = f"{NAMESPACE}.{table_name}"

    if catalog.table_exists(full_name):
        table = catalog.load_table(full_name)
        table.append(arrow_table)
        print(f"appended {len(df)} rows -> {full_name}")
    else:
        table = catalog.create_table(full_name, schema=arrow_table.schema)
        table.append(arrow_table)
        print(f"created {full_name}, loaded {len(df)} rows")


def main():
    catalog = get_catalog()
    ensure_namespace(catalog, NAMESPACE)

    for table_name, path in FILES.items():
        load_csv_to_iceberg(catalog, table_name, path)


if __name__ == "__main__":
    main()
