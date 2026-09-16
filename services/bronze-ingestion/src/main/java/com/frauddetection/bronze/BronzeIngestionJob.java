package com.frauddetection.bronze;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * Dedicated bronze-ingestion job (Option 2): reads transactions.raw and
 * identities.raw and writes each, untransformed, into its own Iceberg
 * table. Runs as its own Flink job with its own checkpoint/consumer
 * group, independent of the feature-computation job — a bug or
 * redeploy in feature logic never stalls the raw archive.
 *
 * Exactly-once mechanics: Flink buffers rows in the Iceberg writer and
 * only commits a new Iceberg snapshot when a checkpoint completes.
 * Kafka offsets are part of that same checkpoint. So a failure rewinds
 * to the last completed checkpoint — the partially-written snapshot
 * for the failed attempt is simply never committed and disappears;
 * readers only ever see fully-committed snapshots. No event lost
 * (offsets only advance after commit succeeds), no duplicate
 * (uncommitted data is never visible).
 */
public class BronzeIngestionJob {

    private static final String NAMESPACE = "bronze";

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        String kafkaBroker = params.get("kafka-broker", "redpanda-0:29092,redpanda-1:29092,redpanda-2:29092");
        String catalogUri = params.get("catalog-uri", "http://iceberg-catalog:8181");
        String warehouse = params.get("warehouse", "s3://iceberg-warehouse/");
        String s3Endpoint = params.get("s3-endpoint", "http://minio:9000");
        String schemaRegistryUrl = params.get("schema-registry-url", "http://redpanda-0:18081");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // This interval IS the Iceberg commit interval — every completed
        // checkpoint triggers exactly one atomic snapshot commit per table.
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000);
        env.getCheckpointConfig().setCheckpointTimeout(60_000);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);
        env.setStateBackend(new EmbeddedRocksDBStateBackend(true));
        env.getCheckpointConfig().setCheckpointStorage(
                new FileSystemCheckpointStorage(warehouse + "flink-checkpoints/"));

        // Idempotent bootstrap: creates the bronze tables on first run only.
        Catalog bootstrapCatalog = IcebergTableBootstrap.buildCatalog(catalogUri, warehouse, s3Endpoint);
        IcebergTableBootstrap.ensureTable(bootstrapCatalog, NAMESPACE, "transactions_raw");
        IcebergTableBootstrap.ensureTable(bootstrapCatalog, NAMESPACE, "identities_raw");

        Map<String, String> catalogProps = new HashMap<>();
        catalogProps.put("uri", catalogUri);
        catalogProps.put("warehouse", warehouse);
        catalogProps.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
        catalogProps.put("s3.endpoint", s3Endpoint);
        catalogProps.put("s3.path-style-access", "true");
        CatalogLoader catalogLoader = CatalogLoader.rest(
                "bronze-catalog", new org.apache.hadoop.conf.Configuration(), catalogProps);

        buildPipeline(env, kafkaBroker, "transactions.raw", "payment-gateway",
                TableIdentifier.of(NAMESPACE, "transactions_raw"), catalogLoader);

        buildPipeline(env, kafkaBroker, "identities.raw", "identity-risk",
                TableIdentifier.of(NAMESPACE, "identities_raw"), catalogLoader);

        env.execute("bronze-ingestion: kafka -> iceberg");
    }

    private static void buildPipeline(StreamExecutionEnvironment env, String kafkaBroker,
                                       String topic, String consumerGroupSuffix,
                                       TableIdentifier tableId, CatalogLoader catalogLoader) {

        // Own consumer group per topic, separate from the feature job's
        // group — this job reads the topic independently and does not
        // interfere with (or depend on) feature-pipeline lag or restarts.
        KafkaSource<RawKafkaRecord> source = KafkaSource.<RawKafkaRecord>builder()
                .setBootstrapServers(kafkaBroker)
                .setTopics(topic)
                .setGroupId("bronze-ingestion-" + consumerGroupSuffix)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(new KafkaAvroRecordDeserializer(schemaRegistryUrl))
                .build();

        DataStream<RawKafkaRecord> raw = env.fromSource(
                source, WatermarkStrategy.noWatermarks(), topic + "-source");

        DataStream<RowData> rows = raw
                .map(BronzeIngestionJob::toRowData)
                .name(topic + "-to-rowdata")
                .returns(RowData.class);

        TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, tableId);

        FlinkSink.forRowData(rows)
                .tableLoader(tableLoader)
                .writeParallelism(2)
                .append();
    }

    private static RowData toRowData(RawKafkaRecord r) {
        GenericRowData row = new GenericRowData(10);
        row.setField(0, r.key != null ? StringData.fromString(r.key) : null);
        row.setField(1, r.value != null ? StringData.fromString(r.value) : null);
        row.setField(2, StringData.fromString(r.topic));
        row.setField(3, r.partition);
        row.setField(4, r.offset);

        Long eventTimeMillis = parseEventTimeHeader(r.headers.get("event-time"));
        row.setField(5, eventTimeMillis != null
                ? TimestampData.fromInstant(Instant.ofEpochMilli(eventTimeMillis))
                : null);

        row.setField(6, TimestampData.fromInstant(Instant.now()));
        row.setField(7, toStringData(r.headers.get("source-service")));
        row.setField(8, toStringData(r.headers.get("run-id")));
        row.setField(9, toStringData(r.headers.get("schema-version")));
        return row;
    }

    private static StringData toStringData(String s) {
        return s != null ? StringData.fromString(s) : null;
    }

    /**
     * The "event-time" header carries the producers' TransactionDT — a
     * simulation-clock offset in seconds, not a real wall-clock epoch
     * (see 03_source_systems.md). We store it as-is for lineage and
     * downstream reconstruction of simulated event order; it is NOT a
     * true calendar timestamp, and this table's event_time column
     * should not be read as one without accounting for that.
     */
    private static Long parseEventTimeHeader(String header) {
        if (header == null || header.equalsIgnoreCase("None")) {
            return null;
        }
        try {
            double seconds = Double.parseDouble(header);
            return (long) (seconds * 1000);
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
