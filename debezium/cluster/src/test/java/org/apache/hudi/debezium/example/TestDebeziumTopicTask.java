package org.apache.hudi.debezium.example;

import org.apache.hudi.debezium.common.DBType;
import org.apache.hudi.debezium.common.TopicConfig;
import org.apache.hudi.debezium.kafka.master.task.IDebeziumTopicTask;
import org.apache.hudi.debezium.zookeeper.connector.ZookeeperConnector;

import static org.apache.hudi.debezium.common.DBType.MySQL;

public class TestDebeziumTopicTask implements IDebeziumTopicTask {

    @Override
    public DBType getDbType() {
        return MySQL;
    }

    @Override
    public IDebeziumTopicTask newInstance() {
        return this;
    }

    @Override
    public void start(String topic, TopicConfig topicConfig, ZookeeperConnector zkConnector) throws Exception {

    }

    @Override
    public void stop() {

    }
}
