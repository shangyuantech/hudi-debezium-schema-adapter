package org.apache.hudi.debezium.kafka.producer.record;

import io.confluent.connect.avro.AvroConverter;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.hudi.debezium.config.KafkaConfig;
import org.apache.hudi.debezium.kafka.producer.record.impl.AvroRecordConverter;
import org.apache.hudi.debezium.kafka.producer.record.impl.StringRecordConverter;

import java.io.IOException;

import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

public class RecordConverterFactory {

    public enum Type {
        KEY,
        VALUE;
    }

    private final static String KEY_CONVERTER = "key.converter";
    private final static String VALUE_CONVERTER = "value.converter";

    private final static String defaultSerClass = "org.apache.kafka.common.serialization.StringSerializer";
    private final static String defaultConverterClass = "org.apache.kafka.connect.storage.StringConverter";

    public static RecordConverter getValueConverter(KafkaConfig kafkaConfig, Type type,
                                                    String serverName, String database, String table) throws IOException {

        String serClass = null;
        String coverClass = null;
        if (type == Type.KEY) {
            serClass = kafkaConfig.getOrDefault(KEY_SERIALIZER_CLASS_CONFIG, defaultSerClass);
            coverClass = kafkaConfig.getOrDefault(KEY_CONVERTER, defaultConverterClass);
        } else {
            serClass = kafkaConfig.getOrDefault(VALUE_SERIALIZER_CLASS_CONFIG, defaultSerClass);
            coverClass = kafkaConfig.getOrDefault(VALUE_CONVERTER, defaultConverterClass);
        }

        if (coverClass.equals(AvroConverter.class.getName()) || serClass.equals(KafkaAvroSerializer.class.getName())) {
            return new AvroRecordConverter(kafkaConfig, type, serverName, database, table);
        } else {
            return new StringRecordConverter(serverName, database, table);
        }
    }
}
