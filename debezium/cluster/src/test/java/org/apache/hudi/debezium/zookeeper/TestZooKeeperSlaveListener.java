package org.apache.hudi.debezium.zookeeper;


import org.apache.curator.test.TestingServer;
import org.apache.hudi.debezium.config.ZookeeperConfig;
import org.apache.hudi.debezium.example.TestSlaveZkService;
import org.apache.hudi.debezium.zookeeper.connector.ZookeeperConnector;
import org.apache.hudi.debezium.zookeeper.slave.SlaveService;
import org.apache.hudi.debezium.zookeeper.util.ZooKeeperUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class TestZooKeeperSlaveListener {

    private TestingServer testingServer;

    @Before
    public void before() throws Exception {
        File tmp = new File("/tmp/zookeeper/test_listener");
        if (tmp.exists()) {
            deleteDir(tmp);
        }
        tmp.mkdirs();
        testingServer = new TestingServer(2181, tmp, true);
    }

    private void deleteDir(File dir) {
        if (dir.isDirectory()) {
            String[] children = dir.list();
            for (int i = 0; i < Objects.requireNonNull(children).length; i++) {
                deleteDir(new File(dir, children[i]));
            }
        }
        dir.delete();
    }

    @Test
    public void testListener() throws Exception {
        ZookeeperConfig zkConfig = new ZookeeperConfig();
        ZookeeperConnector zkConnector = new ZookeeperConnector(zkConfig);
        zkConnector.createZkClient();

        String listenPath = "/debezium/topics";
        zkConnector.createNode(listenPath + "/cluster_mysql_test");

        SlaveService slave = new SlaveService(zkConnector, new TestSlaveZkService(listenPath));
        slave.startSlave();

        Thread.sleep(TimeUnit.SECONDS.toMillis(2));
        zkConnector.createNode(listenPath + "/cluster_mysql_test/cluster_mysql_test.test_database.test_table");
        zkConnector.createNode(listenPath + "/cluster_mysql_test/cluster_mysql_test.test_database.test_table/cluster_mysql_test.test_database.test_table#task_1", "data");

        Thread.sleep(TimeUnit.SECONDS.toMillis(10));
    }
}
