package org.apache.hudi.debezium.mysql;

import com.fasterxml.jackson.databind.ObjectMapper;
import net.mguenther.kafka.junit.*;
import org.apache.curator.test.TestingServer;
import org.apache.hudi.debezium.common.DBType;
import org.apache.hudi.debezium.common.TopicConfig;
import org.apache.hudi.debezium.kafka.master.MasterDebeziumService;
import org.apache.hudi.debezium.kafka.master.task.DebeziumTopicTaskPrototype;
import org.apache.hudi.debezium.mysql.data.SchemaChange;
import org.apache.hudi.debezium.mysql.impl.MySQLDebeziumTopicTask;
import org.apache.hudi.debezium.mysql.impl.MySQLSlaveZkService;
import org.apache.hudi.debezium.zookeeper.connector.ZookeeperConfig;
import org.apache.hudi.debezium.zookeeper.connector.ZookeeperConnector;
import org.apache.hudi.debezium.zookeeper.master.MasterService;
import org.apache.hudi.debezium.zookeeper.slave.SlaveService;
import org.apache.zookeeper.CreateMode;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Collections;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static net.mguenther.kafka.junit.EmbeddedKafkaCluster.provisionWith;
import static net.mguenther.kafka.junit.ObserveKeyValues.on;
import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.CommonClientConfigs.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

public class TestMySQLClusterService {

    private TestingServer testingServer;

    private EmbeddedKafkaCluster kafka;

    @Before
    public void before() throws Exception {
        // start zookeeper
        File tmp = new File("/tmp/zookeeper/test_mysqlcluster");
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
        dbTopicTaskPrototype.addDebeziumTopicTask(DBType.MySQL, new MySQLDebeziumTopicTask());

        MasterDebeziumService masterDebeziumService = new MasterDebeziumService(zkConnector, dbTopicTaskPrototype);
        MasterService master = new MasterService(zkConnector, masterDebeziumService);
        master.startMaster();

        SlaveService slave = new SlaveService(zkConnector, new MySQLSlaveZkService());
        slave.startSlave();

        TopicConfig topicConfig = new TopicConfig();
        topicConfig.setDbType(DBType.MySQL);

        EmbeddedKafkaClusterConfig.useDefaults().getKafkaConfig().getBrokerProperties()
                .forEach((k, v) -> topicConfig.addKafkaConfig(String.valueOf(k), String.valueOf(v)));

        topicConfig.addKafkaConfig(BOOTSTRAP_SERVERS_CONFIG, kafka.getBrokerList());
        topicConfig.addKafkaConfig(KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        topicConfig.addKafkaConfig(VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        topicConfig.addKafkaConfig(GROUP_ID_CONFIG, "test_group");
        topicConfig.addKafkaConfig(AUTO_OFFSET_RESET_CONFIG, "earliest");

        ObjectMapper objectMapper = new ObjectMapper();
        String topicConfigStr = objectMapper.writeValueAsString(topicConfig);
        zkConnector.createNode(String.format("%s/test_topic", masterDebeziumService.getTopicPath()),
                topicConfigStr, CreateMode.EPHEMERAL);

        // send topic a data
        SchemaChange schemaChange = new SchemaChange();
        schemaChange.setDatabaseName("test");
        schemaChange.setDdl("");
        schemaChange.getSource().setServer("cluster_mysql_bigdata");
        schemaChange.getPosition().setFile("mysql-bin.000003");
        schemaChange.getPosition().setPos(4132448L);
        schemaChange.getPosition().setServerId("1");
        schemaChange.getPosition().setTsSec(System.currentTimeMillis());

        KeyValue<String, String> record = new KeyValue<>("bigdata", objectMapper.writeValueAsString(schemaChange));
        kafka.send(SendKeyValues.to("test_topic", Collections.singletonList(record)).build());
        //kafka.observe(on("test_topic", 1).build());

        Thread.sleep(TimeUnit.SECONDS.toMillis(30));
    }

}
