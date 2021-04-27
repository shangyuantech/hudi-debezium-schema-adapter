package org.apache.hudi.debezium.zookeeper.slave.task;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.hudi.debezium.common.DBType;
import org.apache.hudi.debezium.common.TopicConfig;

public interface ISlaveTask {

    DBType getDbType();

    void eventTrigger(CuratorFramework client, TreeCacheEvent event, String topicName, TopicConfig TopicConfig) throws Exception;
}
