package org.apache.hudi.debezium.kafka.producer.record.impl;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang.StringUtils;
import org.apache.hudi.debezium.config.KafkaConfig;
import org.apache.hudi.debezium.kafka.producer.record.RecordConverter;
import org.apache.hudi.debezium.kafka.producer.record.RecordConverterFactory;
import org.apache.hudi.debezium.kafka.util.KafkaConnectUtils;

import java.io.IOException;
import java.util.Map;

public class AvroRecordConverter extends RecordConverter<Object> {

    private Schema schema;

    public AvroRecordConverter(KafkaConfig kafkaConfig, RecordConverterFactory.Type type,
                               String serverName, String database, String table) throws IOException {
        super(serverName, database, table);

        String schemaRegistryUrl = kafkaConfig.get("schema.registry.url");
        if (StringUtils.isBlank(schemaRegistryUrl)) {
            schemaRegistryUrl = kafkaConfig.get("value.converter.schema.registry.url");
        }
        if (StringUtils.isBlank(schemaRegistryUrl)) {
            schemaRegistryUrl = kafkaConfig.get("key.converter.schema.registry.url");
        }

        schema = KafkaConnectUtils.getAvroSchema(schemaRegistryUrl,
                String.format("%s.%s.%s-%s", serverName, database, table, type.name().toLowerCase()));
    }

    @Override
    public KafkaAvroSerializer getSerializer() {
        return new KafkaAvroSerializer();
    }

    @Override
    public Object getValue(Map<String, Object> value, Map<String, String> type) {
        GenericRecord record = new GenericData.Record(schema);
        return value;
    }

    @Override
    public Object getKey(Object key) {
        return null;
    }
}
