package org.apache.hudi.debezium.reflection.impl;

import org.apache.hudi.debezium.common.DBType;
import org.apache.hudi.debezium.common.TopicConfig;
import org.apache.hudi.debezium.kafka.master.task.IDebeziumTopicTask;
import org.apache.hudi.debezium.zookeeper.connector.ZookeeperConnector;

import static org.apache.hudi.debezium.common.DBType.None;

public class TestDebeziumTopicTask implements IDebeziumTopicTask {

    @Override
    public DBType getDbType() {
        return None;
    }

    @Override
    public IDebeziumTopicTask newInstance() {
        return this;
    }

    @Override
    public void start(String topic, TopicConfig topicConfig, ZookeeperConnector zkConnector) throws Exception {

    }

    @Override
    public void stop() throws Exception {

    }
}
