package org.apache.hudi.debezium.zookeeper.slave;

import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.hudi.debezium.zookeeper.connector.ZookeeperConfig;
import org.apache.hudi.debezium.zookeeper.connector.ZookeeperConnector;
import org.apache.hudi.debezium.zookeeper.util.ZooKeeperUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

import static org.apache.zookeeper.CreateMode.EPHEMERAL;

public class SlaveService {

    private final static Logger logger = LoggerFactory.getLogger(SlaveService.class);

    private final ZookeeperConnector zkConnector;

    private final String slavePath;

    private final SlaveZkService slaveZkService;

    public SlaveService(ZookeeperConnector zkConnector, SlaveZkService slaveZkService) {
        this.zkConnector = zkConnector;
        this.slaveZkService = slaveZkService;
        slavePath = ZooKeeperUtils.getSlavePath(zkConnector.getConfig().getService());
    }

    public String getSlavePath() {
        return slavePath;
    }

    public void startSlave() throws Exception {

        registerSlave();

        startDaemonListener();
    }

    private void registerSlave() throws Exception {
        zkConnector.createNode(slavePath, null, EPHEMERAL);

        final ZookeeperConfig zkConf = zkConnector.getConfig();
        TreeCache treeCache = new TreeCache(zkConnector.getClient(), slavePath);

        logger.info("start slave service to receive task ...");
        if (zkConf.getCuratorSingle()) {
            treeCache.getListenable().addListener(slaveZkService::action);
        } else {
            ExecutorService pool = new ThreadPoolExecutor(zkConf.getCuratorCorePoolSize(),
                    zkConf.getCuratorMaxPoolSize(),
                    zkConf.getCuratorKeepAliveSeconds(),
                    TimeUnit.SECONDS,
                    new LinkedBlockingQueue<>(zkConf.getCuratorQueuingCapacity()),
                    Executors.defaultThreadFactory());
            treeCache.getListenable().addListener(slaveZkService::action, pool);
        }

        treeCache.start();
    }

    private void startDaemonListener() throws Exception {
        TreeCache treeCache = new TreeCache(zkConnector.getClient(),
                ZooKeeperUtils.getSlaveBasePath(zkConnector.getConfig().getService()));
        treeCache.getListenable().addListener((client, event) -> {
            ChildData data = event.getData();
            if (data != null && (event.getType().equals(TreeCacheEvent.Type.NODE_REMOVED))) {
                String path = data.getPath();
                if (path.equals(slavePath)) {
                    if (client.checkExists().forPath(path) == null) {
                        logger.warn("Failed to find register node. Try to register node again {}", path);
                        registerSlave();
                    }
                }
            }
        });

        treeCache.start();
    }
}
