package org.apache.hudi.debezium.zookeeper.slave;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.hudi.debezium.zookeeper.util.ZooKeeperUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Pattern;

public abstract class SlaveZkService {

    private final String listenPath;

    private final String listenPathRegularExpression;

    public SlaveZkService(String listenPath) {
        this.listenPath = listenPath;
        this.listenPathRegularExpression = listenPath + "/((?!/).)*/((?!/).)*/((?!/).)*";
    }

    public String getListenPath() {
        return listenPath;
    }

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    public void action(CuratorFramework client, TreeCacheEvent event) {
        ChildData data = event.getData();
        if (data != null) {
            String path = data.getPath();
            switch (event.getType()) {
                case NODE_ADDED:
                    if (createExecutor(client, path)) {
                        addTrigger(client, event);
                    }
                    break;
                case NODE_UPDATED:
                case NODE_REMOVED:
                    if (logger.isDebugEnabled()) {
                        logger.debug("[slave] Path {} trigger {} with data = {}", path, event.getType(), new String(data.getData()));
                    }
                    break;
                default:
                    break;
            }
        }
    }

    private String getExecutorPath(String path) {
        return path + "/executor";
    }

    private boolean createExecutor(CuratorFramework client, String path) {
        if (Pattern.matches(listenPathRegularExpression, path)) {
            try {
                String executorPath = getExecutorPath(path);
                logger.info("[slave] Get a task to run in {}", path);
                client.create()
                        .withMode(CreateMode.EPHEMERAL)
                        .forPath(executorPath, ZooKeeperUtils.getInnetIp().getBytes());
                return true;
            } catch (Exception e) {
                if (e instanceof KeeperException.BadVersionException || e instanceof KeeperException.NodeExistsException) {
                    logger.warn("[slave] Task has been claimed by other slave！", e);
                } else {
                    logger.error("[slave] Error in claim task", e);
                }
                return false;
            }
        } else {
            return false;
        }
    }

    protected abstract void addTrigger(CuratorFramework client, TreeCacheEvent event);

    protected abstract void updateTrigger(CuratorFramework client, TreeCacheEvent event);

    protected abstract void removeTrigger(CuratorFramework client, TreeCacheEvent event);
}
