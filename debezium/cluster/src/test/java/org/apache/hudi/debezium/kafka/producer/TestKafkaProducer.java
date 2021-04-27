package org.apache.hudi.debezium.kafka.producer;

import net.mguenther.kafka.junit.EmbeddedKafkaCluster;
import net.mguenther.kafka.junit.EmbeddedKafkaClusterConfig;
import org.apache.hudi.debezium.config.KafkaConfig;
import org.apache.hudi.debezium.kafka.producer.record.RecordConverterFactory;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Before;
import org.junit.Test;

import static net.mguenther.kafka.junit.EmbeddedKafkaCluster.provisionWith;
import static net.mguenther.kafka.junit.ObserveKeyValues.on;
import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.CommonClientConfigs.GROUP_ID_CONFIG;

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
        //kafkaConfig.addKafkaConfig(KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        //kafkaConfig.addKafkaConfig(VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        kafkaConfig.addKafkaConfig(GROUP_ID_CONFIG, "test_group");

        Converter keyConverter = RecordConverterFactory.getValueConverter(kafkaConfig, RecordConverterFactory.Type.KEY);
        Converter valueConverter = RecordConverterFactory.getValueConverter(kafkaConfig, RecordConverterFactory.Type.VALUE);

        ChangeDataProducer consumer = new ChangeDataProducer(kafkaConfig, keyConverter, valueConverter);

        String key = "key";
        String value = "value";

        SchemaAndValue keyS = keyConverter.toConnectData("test_topic", key.getBytes());
        SchemaAndValue valueS = valueConverter.toConnectData("test_topic", value.getBytes());

        SourceRecord record = new SourceRecord(null, null, "test_topic",
                keyS.schema(), keyS.value(), valueS.schema(), valueS.value());
        consumer.produce(record);
        consumer.close();

        kafka.observe(on("test_topic", 1).build());
    }
}
