package com.frauddetection.bronze;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.types.Types;

import java.util.HashMap;
import java.util.Map;

/**
 * Bronze layer schema: one row per Kafka record, raw value untouched,
 * plus the fields needed to trace a row back to its exact Kafka
 * coordinate and to reconstruct producer lineage. No business columns
 * here on purpose — the transaction/identity JSON stays inside
 * kafka_value, parsed only by a downstream silver-layer job.
 */
public class IcebergTableBootstrap {

    public static final Schema BRONZE_SCHEMA = new Schema(
            Types.NestedField.optional(1, "kafka_key", Types.StringType.get()),
            Types.NestedField.optional(2, "kafka_value", Types.StringType.get()),
            Types.NestedField.required(3, "kafka_topic", Types.StringType.get()),
            Types.NestedField.required(4, "kafka_partition", Types.IntegerType.get()),
            Types.NestedField.required(5, "kafka_offset", Types.LongType.get()),
            Types.NestedField.optional(6, "event_time", Types.TimestampType.withZone()),
            Types.NestedField.required(7, "ingest_time", Types.TimestampType.withZone()),
            Types.NestedField.optional(8, "source_service", Types.StringType.get()),
            Types.NestedField.optional(9, "run_id", Types.StringType.get()),
            Types.NestedField.optional(10, "schema_version", Types.StringType.get())
    );

    public static Catalog buildCatalog(String catalogUri, String warehouse, String s3Endpoint) {
        RESTCatalog catalog = new RESTCatalog();
        Map<String, String> props = new HashMap<>();
        props.put("uri", catalogUri);
        props.put("warehouse", warehouse);
        props.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
        props.put("s3.endpoint", s3Endpoint);
        props.put("s3.path-style-access", "true");
        catalog.setConf(new Configuration());
        catalog.initialize("bronze-catalog", props);
        return catalog;
    }

    /** Idempotent: returns the existing table if already created, else creates it. */
    public static Table ensureTable(Catalog catalog, String namespace, String tableName) {
        Namespace ns = Namespace.of(namespace);

        // namespaceExists()/createNamespace() live on SupportsNamespaces,
        // NOT on the base Catalog interface — calling them directly on a
        // Catalog-typed reference does not compile. Check the interface
        // first, cast only if it's actually implemented (RESTCatalog does).
        if (catalog instanceof SupportsNamespaces) {
            SupportsNamespaces nsCatalog = (SupportsNamespaces) catalog;
            if (!nsCatalog.namespaceExists(ns)) {
                nsCatalog.createNamespace(ns);
            }
        }

        TableIdentifier id = TableIdentifier.of(ns, tableName);
        if (catalog.tableExists(id)) {
            return catalog.loadTable(id);
        }

        // Hourly buckets on event_time: keeps compaction and time-range
        // queries ("give me last night's raw traffic") cheap without
        // over-fragmenting into too many small partitions.
        PartitionSpec spec = PartitionSpec.builderFor(BRONZE_SCHEMA)
                .hour("event_time")
                .build();

        return catalog.createTable(id, BRONZE_SCHEMA, spec);
    }
}
