package org.apache.hudi.debezium.zookeeper.slave;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.hudi.debezium.common.DBType;
import org.apache.hudi.debezium.common.TopicConfig;
import org.apache.hudi.debezium.util.JsonUtils;
import org.apache.hudi.debezium.zookeeper.slave.task.ISlaveTask;
import org.apache.hudi.debezium.zookeeper.slave.task.SlaveTaskPrototype;
import org.apache.hudi.debezium.zookeeper.util.ZooKeeperUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.regex.Pattern;

public class SlaveZkService {

    private final String listenPath;

    private final String listenPathRegularExpression;

    private final SlaveTaskPrototype slaveTaskPrototype;

    public SlaveZkService(String listenPath, SlaveTaskPrototype slaveTaskPrototype) {
        this.listenPath = listenPath;
        this.slaveTaskPrototype = slaveTaskPrototype;
        this.listenPathRegularExpression = listenPath + "/((?!/).)*/((?!/).)*/((?!/).)*";
    }

    public String getListenPath() {
        return listenPath;
    }

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    public void action(CuratorFramework client, TreeCacheEvent event) throws Exception {
        ChildData data = event.getData();
        if (data != null) {
            String path = data.getPath();
            switch (event.getType()) {
                case NODE_ADDED:
                    if (createExecutor(client, path)) {

                        // get topic config
                        Path topic = Paths.get(path).getParent().getParent();
                        String topicPath = topic.toString();
                        String topicName = topic.getFileName().toString();
                        String topicConfigStr = new String(client.getData().forPath(topicPath));
                        TopicConfig topicConfig = JsonUtils.readValue(topicConfigStr, TopicConfig.class);

                        // get task db type;
                        ISlaveTask slaveTask = slaveTaskPrototype.getSlaveTask(topicConfig.getDbType());
                        // start task
                        slaveTask.eventTrigger(client, event, topicName, topicConfig);
                    }
                    break;
                case NODE_UPDATED:
                case NODE_REMOVED:
                    if (logger.isTraceEnabled()) {
                        logger.trace("[slave] Path {} trigger {} with data = {}", path, event.getType(),
                                new String(data.getData()));
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
                if (e instanceof KeeperException.BadVersionException ||
                        e instanceof KeeperException.NodeExistsException) {
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
}
