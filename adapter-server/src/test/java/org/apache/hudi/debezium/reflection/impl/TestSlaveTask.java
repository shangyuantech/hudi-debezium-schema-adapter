package org.apache.hudi.debezium.reflection.impl;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.hudi.debezium.common.DBType;
import org.apache.hudi.debezium.common.TopicConfig;
import org.apache.hudi.debezium.zookeeper.slave.task.ISlaveTask;

public class TestSlaveTask implements ISlaveTask {

    @Override
    public DBType getDbType() {
        return DBType.None;
    }

    @Override
    public void eventTrigger(CuratorFramework client, TreeCacheEvent event, String topicName, TopicConfig TopicConfig) throws Exception {

    }

}
