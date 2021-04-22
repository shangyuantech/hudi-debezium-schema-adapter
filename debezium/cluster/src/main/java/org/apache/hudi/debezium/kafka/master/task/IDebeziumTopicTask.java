package org.apache.hudi.debezium.kafka.master.task;

import org.apache.hudi.debezium.common.DBType;
import org.apache.hudi.debezium.common.TopicConfig;
import org.apache.hudi.debezium.zookeeper.connector.ZookeeperConnector;

public interface IDebeziumTopicTask {

    DBType getDbType();

    IDebeziumTopicTask newInstance();

    void start(String topic, TopicConfig topicConfig, ZookeeperConnector zkConnector)  throws Exception ;

    void stop() throws Exception ;
}
