package org.apache.hudi.debezium.zookeeper;

import static net.mguenther.kafka.junit.EmbeddedKafkaCluster.provisionWith;

import net.mguenther.kafka.junit.EmbeddedKafkaClusterConfig;
import net.mguenther.kafka.junit.EmbeddedKafkaCluster;
import org.apache.curator.test.TestingServer;
import org.apache.hudi.debezium.common.DBType;
import org.apache.hudi.debezium.common.TopicConfig;
import org.apache.hudi.debezium.example.TestDebeziumTopicTask;
import org.apache.hudi.debezium.kafka.master.MasterDebeziumService;
import org.apache.hudi.debezium.kafka.master.task.DebeziumTopicTaskPrototype;
import org.apache.hudi.debezium.config.ZookeeperConfig;
import org.apache.hudi.debezium.util.JsonUtils;
import org.apache.hudi.debezium.zookeeper.connector.ZookeeperConnector;
import org.apache.hudi.debezium.example.TestSlaveZkService;
import org.apache.hudi.debezium.zookeeper.master.MasterService;
import org.apache.hudi.debezium.zookeeper.slave.SlaveService;
import org.apache.hudi.debezium.zookeeper.util.ZooKeeperUtils;
import org.apache.zookeeper.CreateMode;
import org.junit.Before;
import org.junit.Test;
//import org.mortbay.jetty.Server;

import java.io.File;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;

public class TestMasterSlave {

    private TestingServer testingServer;

    private EmbeddedKafkaCluster kafka;

    @Before
    public void before() throws Exception {
        // start zookeeper
        File tmp = new File("/tmp/zookeeper/test_masterslave");
        if (tmp.exists()) {
            deleteDir(tmp);
        }
        tmp.mkdirs();

        testingServer = new TestingServer(2181, tmp);
        testingServer.start();

        // start kafka
        kafka = provisionWith(EmbeddedKafkaClusterConfig.useDefaults());
        kafka.start();
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
    public void testMasterSlave() throws Exception {
        ZookeeperConfig zkConfig = new ZookeeperConfig();
        ZookeeperConnector zkConnector = new ZookeeperConnector(zkConfig);
        zkConnector.createZkClient();

        DebeziumTopicTaskPrototype dbTopicTaskPrototype = new DebeziumTopicTaskPrototype();
        dbTopicTaskPrototype.addDebeziumTopicTask(DBType.MySQL, new TestDebeziumTopicTask());

        MasterDebeziumService masterDebeziumService = new MasterDebeziumService(zkConnector, dbTopicTaskPrototype,
                null);
        MasterService master = new MasterService(zkConnector, masterDebeziumService);
        master.startMaster();

        ZookeeperConnector zkConnector2 = new ZookeeperConnector(zkConfig);
        zkConnector2.createZkClient();
        SlaveService slave = new SlaveService(zkConnector2,
                new TestSlaveZkService(ZooKeeperUtils.getTopicsPath(zkConnector.getConfig().getService())));
        slave.startSlave();

        TopicConfig topicConfig = new TopicConfig();
        topicConfig.setDbType(DBType.MySQL);
        topicConfig.addKafkaConfig(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        String topicConfigStr = JsonUtils.writeValueAsString(topicConfig);
        zkConnector.createNode(masterDebeziumService.getTopicPath() + "/cluster_mysql_test",  topicConfigStr);

        //Server server = new Server();
        //server.start();
        //server.join();
        Thread.sleep(TimeUnit.SECONDS.toMillis(2));
        zkConnector.createNode(masterDebeziumService.getTopicPath() + "/cluster_mysql_test/cluster_mysql_test.test_database.test_table");
        zkConnector.createNode(masterDebeziumService.getTopicPath() + "/cluster_mysql_test/cluster_mysql_test.test_database.test_table/cluster_mysql_test.test_database.test_table#task_1", "data");

        Thread.sleep(TimeUnit.SECONDS.toMillis(20));
        //server.stop();
    }
}
