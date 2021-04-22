package org.apache.hudi.debezium.mysql.impl.slave;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.hudi.debezium.zookeeper.slave.SlaveZkService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySQLSlaveZkService extends SlaveZkService {

    private final static Logger logger = LoggerFactory.getLogger(MySQLSlaveZkService.class);

    public MySQLSlaveZkService(String listenPath) {
        super(listenPath);
    }

    @Override
    protected void addTrigger(CuratorFramework client, TreeCacheEvent event) {
        if (logger.isDebugEnabled()) {
            logger.debug("[slave] start to run task with data = {}", new String(event.getData().getData()));
        }
    }

    @Override
    protected void updateTrigger(CuratorFramework client, TreeCacheEvent event) {

    }

    @Override
    protected void removeTrigger(CuratorFramework client, TreeCacheEvent event) {

    }
}
