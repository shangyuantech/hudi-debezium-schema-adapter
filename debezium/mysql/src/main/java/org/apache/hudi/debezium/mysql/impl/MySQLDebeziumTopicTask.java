package org.apache.hudi.debezium.mysql.impl;

import org.apache.hudi.debezium.common.DBType;
import org.apache.hudi.debezium.common.TopicConfig;
import org.apache.hudi.debezium.kafka.config.KafkaConfig;
import org.apache.hudi.debezium.kafka.consumer.ConsumerService;
import org.apache.hudi.debezium.kafka.master.task.IDebeziumTopicTask;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import static org.apache.hudi.debezium.common.DBType.MySQL;

public class MySQLDebeziumTopicTask implements IDebeziumTopicTask {

    private ConsumerService consumer;

    @Override
    public DBType getDbType() {
        return MySQL;
    }

    @Override
    public IDebeziumTopicTask newInstance() {
        return new MySQLDebeziumTopicTask();
    }

    private final static String defaultDesClass = "org.apache.kafka.common.serialization.StringDeserializer";

    @Override
    public void start(String topic, TopicConfig topicConfig) throws Exception {
        MySQLRecordService recordService = new MySQLRecordService();
        KafkaConfig kafkaConfig = new KafkaConfig(topicConfig.getKafkaConfigProperties());
        Class<?> keyDesClass = Class.forName(kafkaConfig.getOrDefault(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, defaultDesClass));
        Class<?> valueDesClass = Class.forName(kafkaConfig.getOrDefault(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, defaultDesClass));
        consumer = new ConsumerService<>(topic, kafkaConfig, recordService, keyDesClass, valueDesClass);
        consumer.start();
    }

    @Override
    public void stop() throws Exception  {
        if (consumer != null) {
            consumer.stopConsumer();
            consumer.interrupt();
        }
    }
}
