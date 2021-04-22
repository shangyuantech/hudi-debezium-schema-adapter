package org.apache.hudi.debezium.zookeeper.slave;

import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.hudi.debezium.config.ZookeeperConfig;
import org.apache.hudi.debezium.zookeeper.connector.ZookeeperConnector;
import org.apache.hudi.debezium.zookeeper.util.ZooKeeperUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

import static org.apache.zookeeper.CreateMode.EPHEMERAL;

public class SlaveService {

    private final static Logger logger = LoggerFactory.getLogger(SlaveService.class);

    private final ZookeeperConnector zkConnector;

    private final SlaveZkService slaveZkService;

    public SlaveService(ZookeeperConnector zkConnector, SlaveZkService slaveZkService) {
        this.zkConnector = zkConnector;
        this.slaveZkService = slaveZkService;
    }

    public void startSlave() throws Exception {
        registerSlave();
    }

    private void registerSlave() throws Exception {
        final ZookeeperConfig zkConf = zkConnector.getConfig();
        final String slavePath = ZooKeeperUtils.getSlavePath(zkConnector.getConfig().getService());
        final String topicPath = ZooKeeperUtils.getTopicsPath(zkConnector.getConfig().getService());

        // create a slave node to let us know have a slave
        zkConnector.createNode(slavePath, null, EPHEMERAL);

        TreeCache treeCache = TreeCache
                .newBuilder(zkConnector.getClient(), topicPath)
                .setMaxDepth(4)
                .build();
        //TreeCache treeCache = new TreeCache(zkConnector.getClient(), topicPath);
        logger.info("[slave] start slave service to receive task in {} ...", topicPath);

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
}
