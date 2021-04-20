package org.apache.hudi.debezium.zookeeper.slave;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;

public abstract class SlaveZkService {

    public void action(CuratorFramework client, TreeCacheEvent event) throws Exception {
        ChildData data = event.getData();
        if (data != null) {
            switch (event.getType()) {
                case NODE_ADDED:
                    addTrigger(client, event);
                    break;
                case NODE_UPDATED:
                    updateTrigger(client, event);
                    break;
                case NODE_REMOVED:
                    removeTrigger(client, event);
                    break;
                default:
                    break;
            }
        }
    }

    protected abstract void addTrigger(CuratorFramework client, TreeCacheEvent event);

    protected abstract void updateTrigger(CuratorFramework client, TreeCacheEvent event);

    protected abstract void removeTrigger(CuratorFramework client, TreeCacheEvent event);
}
