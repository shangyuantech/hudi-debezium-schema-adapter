package org.apache.hudi.debezium.kafka.producer;

import org.apache.hudi.debezium.config.KafkaConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.Converter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class ChangeDataProducer {

    private final static Logger logger = LoggerFactory.getLogger(ChangeDataProducer.class);

    private final KafkaProducer<Object, Object> producer;

    private final Converter keyConverter;

    private final Converter valueConverter;

    public ChangeDataProducer(KafkaConfig kafkaConfig, Converter keyConverter, Converter valueConverter) {
        ByteArraySerializer serializer = new ByteArraySerializer();
        producer = new KafkaProducer<>((Map) kafkaConfig.getProps(), serializer, serializer);
        this.keyConverter = keyConverter;
        this.valueConverter = valueConverter;
    }

    public void produce(SourceRecord record) {
        produce(record, false);
    }

    public void produce(SourceRecord record, boolean needFlush) {
        byte[] key = keyConverter.fromConnectData(record.topic(), record.keySchema(), record.key());
        byte[] value = valueConverter.fromConnectData(record.topic(), record.valueSchema(), record.value());
        ProducerRecord<Object, Object> sendData = new ProducerRecord<>(record.topic(), key, value);
        try {
            producer.send(sendData);
        } catch(SerializationException e) {
            e.printStackTrace();
        }

        if (needFlush) producer.flush();
    }

    public void batchProduce(List<SourceRecord> records) {
        long rows = records.size();
        if (logger.isDebugEnabled()) {
            if (rows > 0) {
                logger.debug("[slave] start send {} rows data to kafka topic {}", records.size(), records.get(0).topic());
            }
        }
        for (int i = 0 ; i < rows; i ++) {
            produce(records.get(i), false);
        }
        producer.flush();
    }

    public void close() {
        producer.close();
    }
}
