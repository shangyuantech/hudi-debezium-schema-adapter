package org.apache.hudi.debezium.example;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.hudi.debezium.zookeeper.slave.task.ISlaveTask;

public class TestSlaveTask implements ISlaveTask {

    @Override
    public void eventTrigger(CuratorFramework client, TreeCacheEvent event) throws Exception {

    }
}
