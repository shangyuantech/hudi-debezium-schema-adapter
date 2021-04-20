package org.apache.hudi.debezium.zookeeper;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.test.TestingServer;
import org.apache.hudi.debezium.zookeeper.connector.ZookeeperConfig;
import org.apache.hudi.debezium.zookeeper.connector.ZookeeperConnector;
import org.apache.zookeeper.data.Stat;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Objects;

public class TestZookeeperConnection {

    private TestingServer testingServer;

    @Before
    public void before() throws Exception {
        File tmp = new File("/tmp/zookeeper/test_connection");
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
    public void testConnect() throws Exception {
        ZookeeperConfig zkConfig = new ZookeeperConfig();
        ZookeeperConnector zkConnector = new ZookeeperConnector(zkConfig);
        zkConnector.createZkClient();

        String testPath = "/test";
        String testData = "test_data";
        zkConnector.createNode(testPath, testData);

        Stat stat = zkConnector.dataExists(testPath);
        Assert.assertNotNull(stat);
        Assert.assertEquals(zkConnector.getChildes("/").size(), 1);
        Assert.assertEquals(zkConnector.getData(testPath), testData);

        zkConnector.delete(testPath, stat.getVersion());
        Assert.assertNull(zkConnector.dataExists(testPath));
    }
}
