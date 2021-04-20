package org.apache.hudi.debezium.kafka.master.task;

import org.apache.hudi.debezium.common.DBType;
import org.apache.hudi.debezium.common.TopicConfig;

public interface IDebeziumTopicTask {

    DBType getDbType();

    IDebeziumTopicTask newInstance();

    void start(String topic, TopicConfig topicConfig);

    void stop();
}
