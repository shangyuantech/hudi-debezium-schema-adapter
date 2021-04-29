package org.apache.hudi.debezium.zookeeper.connector;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.hudi.debezium.config.ZookeeperConfig;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ZookeeperConnector {

    private static final Logger logger = LoggerFactory.getLogger(ZookeeperConnector.class);

    private final ZookeeperConfig config;

    private CuratorFramework client;

    public ZookeeperConnector(ZookeeperConfig config) {
        this(config, true);
    }

    public ZookeeperConnector(ZookeeperConfig config, boolean startClient) {
        this.config = config;
        if (startClient) createZkClient();
    }

    public CuratorFramework createZkClient() {
        if (client != null) {
            return client;
        }
        logger.info("init curator framework client ...");
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(
                config.getZkClientBaseSleepTimeMs(),
                config.getZkClientMaxRetries());
        client = CuratorFrameworkFactory.builder()
                .connectString(config.getZkConnPath())
                .sessionTimeoutMs(config.getZkClientSessionTimeout())
                .connectionTimeoutMs(config.getZkClientConnectionTimeout())
                .retryPolicy(retryPolicy)
                .namespace(config.getNamespace())
                .build();
        client.start();
        return client;
    }

    public CuratorFramework getClient() {
        if (client != null) {
            return client;
        } else {
            return createZkClient();
        }
    }

    public ZookeeperConfig getConfig() {
        return config;
    }

    public List<String> getChildes(String path) throws Exception {
        return client.getChildren().forPath(path);
    }

    public Stat dataExists(String path) throws Exception {
        return dataExists(path, false);
    }

    public Stat dataExists(String path, boolean watch) throws Exception {
        return watch ? client.checkExists().watched().forPath(path)
                : client.checkExists().forPath(path);
    }

    public void createNode(String path, String data, CreateMode createMode) throws Exception {
        logger.debug("create node " + path);
        client.create()
                .creatingParentsIfNeeded()
                .withMode(createMode)
                .forPath(path, data == null ? null : data.getBytes());
    }

    public void createNode(String path, String data) throws Exception {
        createNode(path, data, CreateMode.PERSISTENT);
    }

    public void createNode(String path) throws Exception {
        createNode(path, null);
    }

    public void delete(String path) throws Exception {
        delete(path, null);
    }

    public void delete(String path, Integer version) throws Exception {
        if (version != null) {
            client.delete()
                    .deletingChildrenIfNeeded()
                    .withVersion(version)
                    .forPath(path);
        } else {
            client.delete()
                    .deletingChildrenIfNeeded()
                    .forPath(path);
        }
    }

    public String getData(String path) throws Exception {
        if (dataExists(path, false) == null) {
            return null;
        }
        byte[] data = client.getData().forPath(path);
        return data == null ? "" : new String(data);
    }

    public void setData(String path, String data, Integer version) throws Exception {
        logger.debug("set node {}, data {}, version {}", path, data, version);
        client.setData()
                .withVersion(version)
                .forPath(path, data == null ? null : data.getBytes());
    }

    public void setData(String path, String data) throws Exception {
        logger.debug("set node {}, data {}", path, data);
        client.setData().forPath(path, data == null ? null : data.getBytes());
    }
}
