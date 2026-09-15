package com.frauddetection.bronze;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * Deserializes at the ConsumerRecord level (not just the value) because
 * the bronze schema needs partition, offset, and the lineage headers
 * (source-service, run-id, schema-version, event-time) that the
 * producers attach — none of which a plain value-only deserializer
 * can see.
 */
public class KafkaRawRecordDeserializer implements KafkaRecordDeserializationSchema<RawKafkaRecord> {

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<RawKafkaRecord> out) {
        String key = record.key() != null ? new String(record.key(), StandardCharsets.UTF_8) : null;
        String value = record.value() != null ? new String(record.value(), StandardCharsets.UTF_8) : null;

        Map<String, String> headers = new HashMap<>();
        for (Header h : record.headers()) {
            if (h.value() != null) {
                headers.put(h.key(), new String(h.value(), StandardCharsets.UTF_8));
            }
        }

        out.collect(new RawKafkaRecord(
                key, value, record.topic(), record.partition(),
                record.offset(), record.timestamp(), headers
        ));
    }

    @Override
    public TypeInformation<RawKafkaRecord> getProducedType() {
        return TypeInformation.of(RawKafkaRecord.class);
    }
}
