package com.frauddetection.bronze;

import java.io.Serializable;
import java.util.Map;

/**
 * Carries one Kafka record through the pipeline untouched, plus the
 * metadata (partition, offset, headers) needed for the bronze/raw
 * Iceberg schema. Deliberately dumb: no parsing of the JSON payload,
 * no business logic. Bronze layer stores what arrived, not what it means.
 */
public class RawKafkaRecord implements Serializable {

    public String key;
    public String value;
    public String topic;
    public int partition;
    public long offset;
    public long kafkaTimestamp;
    public Map<String, String> headers;

    public RawKafkaRecord() {
        // required for Flink POJO serialization
    }

    public RawKafkaRecord(String key, String value, String topic, int partition,
                           long offset, long kafkaTimestamp, Map<String, String> headers) {
        this.key = key;
        this.value = value;
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.kafkaTimestamp = kafkaTimestamp;
        this.headers = headers;
    }
}
