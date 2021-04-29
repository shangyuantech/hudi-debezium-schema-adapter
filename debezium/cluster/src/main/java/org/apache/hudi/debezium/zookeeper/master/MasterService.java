package org.apache.hudi.debezium.zookeeper.master;

import org.apache.curator.framework.recipes.leader.*;
import org.apache.hudi.debezium.zookeeper.connector.ZookeeperConnector;
import org.apache.hudi.debezium.zookeeper.util.ZooKeeperUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MasterService {

    private final static Logger logger = LoggerFactory.getLogger(MasterService.class);

    private final ZookeeperConnector zkConnector;

    private final IMasterZkService masterZkService;

    public MasterService(ZookeeperConnector zkConnector, IMasterZkService masterZkService) {
        this.zkConnector = zkConnector;
        this.masterZkService = masterZkService;
    }

    public void startMaster() throws Exception {
        logger.info("start a leader selector to choose master ...");
        String leaderPath = ZooKeeperUtils.getMaterPath(zkConnector.getConfig().getService());
        LeaderLatch leaderLatch = new LeaderLatch(zkConnector.getClient(), leaderPath);
        leaderLatch.start();

        leaderLatch.addListener(new LeaderLatchListener() {
            @Override
            public void isLeader() {
                logger.info("[master] start master service to collect topic data ...");
                try {
                    masterZkService.isLeader();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            @Override
            public void notLeader() {
                logger.info("[master] now it is not a master node ...");
                try {
                    masterZkService.notLeader();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        //leaderLatch.await();
    }
}
