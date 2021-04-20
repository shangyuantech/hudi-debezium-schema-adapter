package org.apache.hudi.debezium.example;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.hudi.debezium.zookeeper.slave.SlaveZkService;

public class TestSlaveZkService extends SlaveZkService {

    @Override
    protected void addTrigger(CuratorFramework client, TreeCacheEvent event) {

    }

    @Override
    protected void updateTrigger(CuratorFramework client, TreeCacheEvent event) {

    }

    @Override
    protected void removeTrigger(CuratorFramework client, TreeCacheEvent event) {

    }
}
