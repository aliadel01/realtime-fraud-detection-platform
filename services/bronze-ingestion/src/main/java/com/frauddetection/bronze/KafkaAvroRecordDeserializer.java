package com.frauddetection.bronze;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * Replaces the old plain-string KafkaRawRecordDeserializer now that
 * producers write Avro (schema-id magic byte + binary payload) instead
 * of raw JSON text.
 *
 * Design choice: decode Avro -> GenericRecord -> re-encode as a JSON
 * string, then store that JSON string in kafka_value, same as before.
 * This means IcebergTableBootstrap's BRONZE_SCHEMA (kafka_value: string)
 * and every downstream silver-layer job need ZERO changes — Avro is an
 * on-the-wire contract between producer and this deserializer only, it
 * never leaks into the Iceberg schema. Bronze layer stays "dumb storage"
 * as documented in RawKafkaRecord.
 *
 * KafkaAvroDeserializer resolves the writer schema itself: it reads the
 * schema-id from the record's magic byte and fetches that exact schema
 * version from the Registry — it does NOT need to know in advance which
 * schema version wrote which record. That's what makes multi-version
 * evolution transparent to this class: an old record (schema v1) and a
 * new record (schema v3, extra optional field) both decode correctly,
 * no code change needed here when a producer adds a field.
 */
public class KafkaAvroRecordDeserializer implements KafkaRecordDeserializationSchema<RawKafkaRecord> {

    private final String schemaRegistryUrl;
    private transient KafkaAvroDeserializer avroDeserializer;

    public KafkaAvroRecordDeserializer(String schemaRegistryUrl) {
        this.schemaRegistryUrl = schemaRegistryUrl;
    }

    private KafkaAvroDeserializer getDeserializer() {
        if (avroDeserializer == null) {
            avroDeserializer = new KafkaAvroDeserializer();
            Map<String, Object> config = new HashMap<>();
            config.put("schema.registry.url", schemaRegistryUrl);
            config.put("specific.avro.reader", false); // GenericRecord, no generated classes needed
            avroDeserializer.configure(config, false);
        }
        return avroDeserializer;
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<RawKafkaRecord> out) throws Exception {
        // key stays a plain string (producers never Avro-encode the key)
        String key = record.key() != null ? new String(record.key(), StandardCharsets.UTF_8) : null;

        String valueJson = null;
        if (record.value() != null) {
            GenericRecord avroRecord = (GenericRecord) getDeserializer()
                    .deserialize(record.topic(), record.value());
            valueJson = toJson(avroRecord);
        }

        Map<String, String> headers = new HashMap<>();
        for (Header h : record.headers()) {
            if (h.value() != null) {
                headers.put(h.key(), new String(h.value(), StandardCharsets.UTF_8));
            }
        }

        out.collect(new RawKafkaRecord(
                key, valueJson, record.topic(), record.partition(),
                record.offset(), record.timestamp(), headers
        ));
    }

    private static String toJson(GenericRecord record) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        JsonEncoder encoder = EncoderFactory.get().jsonEncoder(record.getSchema(), baos);
        new SpecificDatumWriter<GenericRecord>(record.getSchema()).write(record, encoder);
        encoder.flush();
        return baos.toString(StandardCharsets.UTF_8.name());
    }

    @Override
    public TypeInformation<RawKafkaRecord> getProducedType() {
        return TypeInformation.of(RawKafkaRecord.class);
    }
}
