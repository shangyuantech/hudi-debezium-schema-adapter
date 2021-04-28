package org.apache.hudi.debezium.zookeeper.slave;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.hudi.debezium.common.TopicConfig;
import org.apache.hudi.debezium.util.JsonUtils;
import org.apache.hudi.debezium.zookeeper.slave.task.ISlaveTask;
import org.apache.hudi.debezium.zookeeper.slave.task.SlaveTaskPrototype;
import org.apache.hudi.debezium.zookeeper.util.ZooKeeperUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class SlaveZkService {

    private final static byte[] RUNNING = "01".getBytes();
    private final static byte[] FINISH = "02".getBytes();
    private final static byte[] ERROR = "03".getBytes();

    private final static List<String> NEED_REDO = new ArrayList<String>() {{
        add(new String(RUNNING));
        add(new String(ERROR));
    }};

    private final String listenPath;

    private final String listenPathRegularExpression;

    private final String statPathRegularExpression;

    private final SlaveTaskPrototype slaveTaskPrototype;

    public SlaveZkService(String listenPath, SlaveTaskPrototype slaveTaskPrototype) {
        this.listenPath = listenPath;
        this.slaveTaskPrototype = slaveTaskPrototype;
        this.listenPathRegularExpression = listenPath + "/((?!/).)*/((?!/).)*/((?!/).)*";
        this.statPathRegularExpression = listenPathRegularExpression + "/stat";
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
                case NODE_UPDATED:
                    if (createExecutor(client, path)) {
                        runTask(client, event, path);
                    }
                    break;
                case NODE_REMOVED:
                    if (checkErrorTask(client, path)) {
                        if (logger.isTraceEnabled()) {
                            logger.trace("[slave] Deleting path {} trigger {} with data = {}", path, event.getType(),
                                    new String(data.getData()));
                        }
                        String taskPath = Paths.get(path).getParent().toString();

                        // redo job
                        if (createExecutor(client, taskPath)) {
                            runTask(client, event, taskPath);
                        }
                    }
                    break;
                default:
                    break;
            }
        }
    }

    private void runTask(CuratorFramework client, TreeCacheEvent event, String path) {
        try {
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

            // finish task, remove executor node and current node
            finishTask(client, path, true);
        } catch (Exception e) {
            // if exception, remove executor node and set state error
            finishTask(client, path, false);
        }
    }

    private String getExecutorPath(String path) {
        return path + "/executor";
    }

    private String getStatPath(String path) {
        return path + "/stat";
    }

    /**
     * tag a stat node so that other slave know if it is finished
     * @param statValue 01 running 02 finish 03 error
     */
    private void setStat(CuratorFramework client, String statPath, byte[] statValue) throws Exception {
        Stat stat = client.checkExists().forPath(statPath);
        if (stat == null) {
            client.create()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(statPath, statValue);
        } else {
            client.setData()
                    .withVersion(stat.getVersion())
                    .forPath(statPath, statValue);
        }
    }

    private boolean createExecutor(CuratorFramework client, String path) {
        if (Pattern.matches(listenPathRegularExpression, path)) {
            try {
                if (client.checkExists().forPath(path) == null) {
                    if (logger.isTraceEnabled()) {
                        logger.trace("[slave] Path {} may be deleted, skip this task", path);
                    }
                    return false;
                }

                // add a executor node
                String executorPath = getExecutorPath(path);
                client.create()
                        .withMode(CreateMode.EPHEMERAL)
                        .forPath(executorPath, ZooKeeperUtils.getInnetIp().getBytes());
                logger.info("[slave] Get a task to run in {}", path);

                // tag a stat node so that other slave know if it is finished
                setStat(client, getStatPath(path), RUNNING);

                return true;
            } catch (Exception e) {
                if (e instanceof KeeperException.BadVersionException ||
                        e instanceof KeeperException.NodeExistsException ||
                        e instanceof KeeperException.NoNodeException) {
                    logger.warn("[slave] Task has been claimed by other slave\n {}", e.getMessage());
                } else {
                    logger.error("[slave] Error in claim task", e);
                }
                return false;
            }
        } else {
            return false;
        }
    }

    private void finishTask(CuratorFramework client, String path, boolean finished) {
        try {
            // tag a finish node so that other slave know it is finished or error
            logger.info("[slave] Task in {} has {}", path, finished ? "finished" : "to be reset and redo");
            setStat(client, getStatPath(path), finished ? FINISH : ERROR);

            // then remove executor node
            client.delete()
                    .deletingChildrenIfNeeded()
                    .forPath(finished ? path : getExecutorPath(path));

            // If it is the last task, delete the whole task
            if (finished) {
                String taskPath = Paths.get(path).getParent().toString();
                if (client.getChildren().forPath(taskPath).size() == 0) {
                    client.delete()
                            .forPath(taskPath);
                }
            }
        } catch (Exception e) {
            logger.error(String.format("[slave] Error when %s job in path %s",
                    finished ? "finish" : "reset", path), e);
        }
    }

    private boolean checkErrorTask(CuratorFramework client, String path) {
        if (Pattern.matches(statPathRegularExpression, path)) {
            // get stat node and check if it is error
            try {
                Stat stat = client.checkExists().forPath(path);
                if (stat == null) {
                    // check if this task has been deleted
                    String subTaskPath = Paths.get(path).getParent().toString();
                    Stat subTaskStat = client.checkExists().forPath(subTaskPath);
                    if (subTaskStat == null) {
                        return false;
                    } else {
                        return true;
                    }
                } else {
                    if (NEED_REDO.contains(new String(client.getData().forPath(path)))) {
                        return true;
                    } else {
                        return false;
                    }
                }
            } catch (Exception e) {
                logger.error(String.format("[slave] Error when check error path %s", path), e);
                return false;
            }
        }

        return false;
    }
}
