package org.apache.hudi.debezium.mysql.impl.slave;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.hudi.debezium.zookeeper.slave.SlaveZkService;

public class MySQLSlaveZkService extends SlaveZkService {

    public MySQLSlaveZkService(String listenPath) {
        super(listenPath);
    }

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
