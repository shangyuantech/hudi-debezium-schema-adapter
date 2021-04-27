package org.apache.hudi.debezium;

import org.apache.hudi.debezium.config.ZookeeperConfig;
import org.apache.hudi.debezium.kafka.master.MasterDebeziumService;
import org.apache.hudi.debezium.reflection.ReflectionService;
import org.apache.hudi.debezium.zookeeper.connector.ZookeeperConnector;
import org.apache.hudi.debezium.zookeeper.master.MasterService;
import org.apache.hudi.debezium.zookeeper.slave.SlaveService;
import org.apache.hudi.debezium.zookeeper.slave.SlaveZkService;
import org.eclipse.jetty.server.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AdapterServer {

    private final static Logger logger = LoggerFactory.getLogger(AdapterServer.class);

    public static void main(String[] args) {
        start();
    }

    public static void start() {
        start(true);
    }

    public static void start(boolean startJetty) {
        // init zookeeper config
        ZookeeperConfig zkConfig = new ZookeeperConfig();
        // get reflection
        ReflectionService reflection = new ReflectionService("org.apache.hudi.debezium");

        // start master
        MasterDebeziumService masterDbzService;
        try {
            logger.info("[master] starting master service ...");
            ZookeeperConnector masterZkConnector = new ZookeeperConnector(zkConfig);
            masterDbzService = new MasterDebeziumService(masterZkConnector,
                    reflection.getDbzTopicTaskPrototype(), reflection.getDbzConfigBuilderPrototype(),
                    true);

            MasterService master = new MasterService(masterZkConnector, masterDbzService);
            master.startMaster();
            logger.info("[master] master service started successfully !");
        } catch (Exception e) {
            logger.error("[master] error when starting master service", e);
            throw new RuntimeException(e);
        }

        // start slave
        try {
            logger.info("[slave] starting slave service ...");
            SlaveService slave = new SlaveService(
                    new ZookeeperConnector(zkConfig, true),
                    new SlaveZkService(masterDbzService.getTopicPath(), reflection.getSlaveTaskPrototype()));
            slave.startSlave();
            logger.info("[slave] slave service started successfully !");
        } catch (Exception e) {
            logger.error("[slave] error when starting slave service", e);
            throw new RuntimeException(e);
        }

        if (!startJetty) {
            return;
        }

        try {
            Server server = new Server();
            server.start();
            server.join();
        } catch (Exception e) {
            logger.error("error when starting jetty service", e);
            throw new RuntimeException(e);
        }
    }
}
