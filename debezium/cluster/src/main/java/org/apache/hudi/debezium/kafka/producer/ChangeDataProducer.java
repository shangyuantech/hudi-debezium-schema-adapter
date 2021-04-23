package org.apache.hudi.debezium.kafka.producer;

import org.apache.hudi.debezium.config.KafkaConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;

import java.util.List;

public class ChangeDataProducer<K, V> {

    private final KafkaProducer<K, V> producer;

    public ChangeDataProducer(KafkaConfig kafkaConfig, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        producer = new KafkaProducer<>(kafkaConfig.getProps(), keySerializer, valueSerializer);
    }

    public void produce(ProducerRecord<K, V> record) {
        producer.send(record);
        producer.flush();
    }

    public void batchProduce(List<ProducerRecord<K, V>> records) {
        for (ProducerRecord<K, V> record : records) {
            producer.send(record);
        }
        producer.flush();
    }

    public void close() {
        producer.close();
    }

    public void produce(String topic, Object key, Object value) {
        ProducerRecord<K, V> record = new ProducerRecord<>(topic, (K) key, (V) value);
        producer.send(record);
        producer.flush();
    }
}
