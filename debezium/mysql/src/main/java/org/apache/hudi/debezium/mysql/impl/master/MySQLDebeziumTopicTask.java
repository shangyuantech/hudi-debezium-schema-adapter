package org.apache.hudi.debezium.mysql.impl.master;

import org.apache.hudi.debezium.common.DBType;
import org.apache.hudi.debezium.common.TopicConfig;
import org.apache.hudi.debezium.config.KafkaConfig;
import org.apache.hudi.debezium.kafka.consumer.ConsumerService;
import org.apache.hudi.debezium.kafka.master.task.IDebeziumTopicTask;
import org.apache.hudi.debezium.zookeeper.connector.ZookeeperConnector;
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
    public void start(String topic, TopicConfig topicConfig, ZookeeperConnector zkConnector) throws Exception {
        KafkaConfig kafkaConfig = new KafkaConfig(topicConfig.getKafkaConfigProperties());
        Class<?> valueDesClass = Class.forName(kafkaConfig.getOrDefault(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, defaultDesClass));
        MySQLRecordService recordService = new MySQLRecordService(topic, topicConfig, kafkaConfig, valueDesClass);

        consumer = new ConsumerService(topic, kafkaConfig, zkConnector, recordService);
        consumer.start();
    }

    @Override
    public void stop() throws Exception  {
        if (consumer != null) {
            consumer.stopConsumer();
        }
    }
}
