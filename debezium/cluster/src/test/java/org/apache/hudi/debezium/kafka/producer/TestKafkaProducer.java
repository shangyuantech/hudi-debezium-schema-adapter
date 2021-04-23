package org.apache.hudi.debezium.kafka.producer;

import net.mguenther.kafka.junit.EmbeddedKafkaCluster;
import net.mguenther.kafka.junit.EmbeddedKafkaClusterConfig;
import org.apache.hudi.debezium.config.KafkaConfig;
import org.apache.hudi.debezium.kafka.producer.record.RecordConverter;
import org.apache.hudi.debezium.kafka.producer.record.RecordConverterFactory;
import org.junit.Before;
import org.junit.Test;

import static net.mguenther.kafka.junit.EmbeddedKafkaCluster.provisionWith;
import static net.mguenther.kafka.junit.ObserveKeyValues.on;
import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.CommonClientConfigs.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

public class TestKafkaProducer {

    private EmbeddedKafkaCluster kafka;

    @Before
    public void before() {
        // start kafka
        kafka = provisionWith(EmbeddedKafkaClusterConfig.useDefaults());
        kafka.start();
    }

    @Test
    public void testKafkaProducer() throws Exception {
        KafkaConfig kafkaConfig = new KafkaConfig();
        EmbeddedKafkaClusterConfig.useDefaults().getKafkaConfig().getBrokerProperties()
                .forEach((k, v) -> kafkaConfig.addKafkaConfig(String.valueOf(k), String.valueOf(v)));

        kafkaConfig.addKafkaConfig(BOOTSTRAP_SERVERS_CONFIG, kafka.getBrokerList());
        kafkaConfig.addKafkaConfig(KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaConfig.addKafkaConfig(VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaConfig.addKafkaConfig(GROUP_ID_CONFIG, "test_group");

        RecordConverter<?> keyConverter = RecordConverterFactory.getValueConverter(kafkaConfig,
                RecordConverterFactory.Type.KEY, "test_topic", "test", "test");
        RecordConverter<?> valueConverter = RecordConverterFactory.getValueConverter(kafkaConfig,
                RecordConverterFactory.Type.VALUE, "test_topic", "test", "test");

        ChangeDataProducer<?, ?> consumer = new ChangeDataProducer<>(kafkaConfig,
                keyConverter.getSerializer(), valueConverter.getSerializer());
        consumer.produce("test_topic", keyConverter.getKey("test"), valueConverter.getKey("test"));
        consumer.close();

        kafka.observe(on("test_topic", 1).build());
    }
}
