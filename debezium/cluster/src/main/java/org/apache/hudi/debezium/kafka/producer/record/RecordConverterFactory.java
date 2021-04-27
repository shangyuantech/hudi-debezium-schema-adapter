package org.apache.hudi.debezium.kafka.producer.record;

import io.confluent.connect.avro.AvroConverter;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.hudi.debezium.config.KafkaConfig;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.StringConverter;

import java.util.Map;

import static org.apache.hudi.debezium.kafka.producer.record.RecordConverterFactory.Type.KEY;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

public class RecordConverterFactory {

    public enum Type {
        KEY,
        VALUE
    }

    private final static String KEY_CONVERTER = "key.converter";
    private final static String VALUE_CONVERTER = "value.converter";

    private final static String defaultSerClass = "org.apache.kafka.common.serialization.StringSerializer";
    private final static String defaultConverterClass = "org.apache.kafka.connect.storage.StringConverter";

    public static Converter getValueConverter(KafkaConfig kafkaConfig, Type type) {

        String serClass = null;
        String coverClass = null;
        if (type == KEY) {
            serClass = kafkaConfig.getOrDefault(KEY_SERIALIZER_CLASS_CONFIG, defaultSerClass);
            coverClass = kafkaConfig.getOrDefault(KEY_CONVERTER, defaultConverterClass);
        } else {
            serClass = kafkaConfig.getOrDefault(VALUE_SERIALIZER_CLASS_CONFIG, defaultSerClass);
            coverClass = kafkaConfig.getOrDefault(VALUE_CONVERTER, defaultConverterClass);
        }

        if (coverClass.equals(AvroConverter.class.getName()) || serClass.equals(KafkaAvroSerializer.class.getName())) {
            Converter avroConverter = new AvroConverter();
            avroConverter.configure((Map) kafkaConfig.getProps(), type == KEY);
            return avroConverter;
        } else {
            Converter stringConverter = new StringConverter();
            stringConverter.configure((Map) kafkaConfig.getProps(), type == KEY);
            return stringConverter;
        }
    }
}
