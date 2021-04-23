package org.apache.hudi.debezium.zookeeper.slave.task;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;

public interface ISlaveTask {

    void eventTrigger(CuratorFramework client, TreeCacheEvent event) throws Exception;
}
