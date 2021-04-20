package org.apache.hudi.debezium.zookeeper;

import static net.mguenther.kafka.junit.EmbeddedKafkaCluster.provisionWith;

import com.fasterxml.jackson.databind.ObjectMapper;
import net.mguenther.kafka.junit.EmbeddedKafkaClusterConfig;
import net.mguenther.kafka.junit.EmbeddedKafkaCluster;
import org.apache.curator.test.TestingServer;
import org.apache.hudi.debezium.common.DBType;
import org.apache.hudi.debezium.common.TopicConfig;
import org.apache.hudi.debezium.example.TestDebeziumTopicTask;
import org.apache.hudi.debezium.kafka.master.MasterDebeziumService;
import org.apache.hudi.debezium.kafka.master.task.DebeziumTopicTaskPrototype;
import org.apache.hudi.debezium.zookeeper.connector.ZookeeperConfig;
import org.apache.hudi.debezium.zookeeper.connector.ZookeeperConnector;
import org.apache.hudi.debezium.example.TestSlaveZkService;
import org.apache.hudi.debezium.zookeeper.master.MasterService;
import org.apache.hudi.debezium.zookeeper.slave.SlaveService;
import org.apache.zookeeper.CreateMode;
import org.junit.Before;
import org.junit.Test;
import org.mortbay.jetty.Server;

import java.io.File;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;

public class TestMasterSlave {

    private TestingServer testingServer;

    private EmbeddedKafkaCluster kafka;

    @Before
    public void before() throws Exception {
        // start zookeeper
        File tmp = new File("/tmp/zookeeper/test_masterslave");
        if (!tmp.exists()) {
            tmp.mkdirs();
        }
        testingServer = new TestingServer(2181, tmp);
        testingServer.start();

        // start kafka
        kafka = provisionWith(EmbeddedKafkaClusterConfig.useDefaults());
        kafka.start();
    }

    @Test
    public void testMasterSlave() throws Exception {
        ZookeeperConfig zkConfig = new ZookeeperConfig();
        ZookeeperConnector zkConnector = new ZookeeperConnector(zkConfig);
        zkConnector.createZkClient();

        DebeziumTopicTaskPrototype dbTopicTaskPrototype = new DebeziumTopicTaskPrototype();
        dbTopicTaskPrototype.addDebeziumTopicTask(DBType.MySQL, new TestDebeziumTopicTask());

        MasterDebeziumService masterDebeziumService = new MasterDebeziumService(zkConnector, dbTopicTaskPrototype);
        MasterService master = new MasterService(zkConnector, masterDebeziumService);
        master.startMaster();

        SlaveService slave = new SlaveService(zkConnector, new TestSlaveZkService());
        slave.startSlave();

        TopicConfig topicConfig = new TopicConfig();
        topicConfig.setDbType(DBType.MySQL);
        topicConfig.addKafkaConfig(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        ObjectMapper objectMapper = new ObjectMapper();
        String topicConfigStr = objectMapper.writeValueAsString(topicConfig);
        zkConnector.createNode(String.format("%s/test_topic", masterDebeziumService.getTopicPath()),
                topicConfigStr, CreateMode.EPHEMERAL);

        Server server = new Server();
        server.start();
        server.join();

        //Thread.sleep(TimeUnit.SECONDS.toMillis(20));
        //server.stop();
    }
}
