package org.apache.hudi.debezium.mysql.cluster;

import ch.vorburger.mariadb4j.DB;
import ch.vorburger.mariadb4j.DBConfigurationBuilder;
import net.mguenther.kafka.junit.*;
import org.apache.curator.test.TestingServer;
import org.apache.hudi.debezium.common.DBType;
import org.apache.hudi.debezium.common.TopicConfig;
import org.apache.hudi.debezium.kafka.connect.DebeziumConfigBuilderPrototype;
import org.apache.hudi.debezium.kafka.master.MasterDebeziumService;
import org.apache.hudi.debezium.kafka.master.task.DebeziumTopicTaskPrototype;
import org.apache.hudi.debezium.mysql.data.MySQLSchemaChange;
import org.apache.hudi.debezium.mysql.impl.connect.MySQLDebeziumConfigBuilder;
import org.apache.hudi.debezium.mysql.impl.master.MySQLDebeziumTopicTask;
import org.apache.hudi.debezium.mysql.impl.slave.MySQLSlaveTask;
import org.apache.hudi.debezium.config.ZookeeperConfig;
import org.apache.hudi.debezium.util.JsonUtils;
import org.apache.hudi.debezium.zookeeper.connector.ZookeeperConnector;
import org.apache.hudi.debezium.zookeeper.master.MasterService;
import org.apache.hudi.debezium.zookeeper.slave.SlaveService;
import org.apache.hudi.debezium.zookeeper.slave.SlaveZkService;
import org.apache.hudi.debezium.zookeeper.slave.task.SlaveTaskPrototype;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Application;
import java.io.File;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static net.mguenther.kafka.junit.EmbeddedKafkaCluster.provisionWith;
import static net.mguenther.kafka.junit.ObserveKeyValues.on;
import static net.mguenther.kafka.junit.SendKeyValues.to;
import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.CommonClientConfigs.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

public class TestMySQLClusterService extends JerseyTest {

    public static final String CONNECTORS = "[\"cluster_mysql_test-test_database-test_table\"]";

    public static final String CONNECTOR_CONFIG1 = "{\n" +
            "\t\"config\": {\n" +
            "\t\t\"connector.class\": \"io.debezium.connector.mysql.MySqlConnector\",\n" +
            "\t\t\"database.history.kafka.bootstrap.servers\": \"PLAINTEXT://localhost:9092\",\n" +
            "\t\t\"database.history.kafka.topic\": \"schema-changes.test_database\",\n" +
            "\t\t\"database.history.store.only.monitored.tables.ddl\": \"true\",\n" +
            "\t\t\"database.hostname\": \"localhost\",\n" +
            "\t\t\"database.password\": \"test\",\n" +
            "\t\t\"database.port\": \"3306\",\n" +
            "\t\t\"database.server.name\": \"cluster_mysql_test\",\n" +
            "\t\t\"database.user\": \"test\",\n" +
            "\t\t\"database.whitelist\": \"test_database\",\n" +
            "\t\t\"include.schema.changes\": \"true\",\n" +
            "\t\t\"name\": \"cluster_mysql_test-test_database-test_table\",\n" +
            "\t\t\"table.whitelist\": \"test_database.test_table\",\n" +
            "\t\t\"time.precision.mode\": \"connect\",\n" +
            "\t\t\"tombstones.on.delete\": \"false\"\n" +
            "\t},\n" +
            "\t\"name\": \"cluster_mysql_test-test_database-test_table\",\n" +
            "\t\"tasks\": [\n" +
            "\t\t{\n" +
            "\t\t\t\"connector\": \"cluster_mysql_test-test_database-test_table\",\n" +
            "\t\t\t\"task\": 0\n" +
            "\t\t}\n" +
            "\t],\n" +
            "\t\"type\": \"source\"\n" +
            "}";


    @Path("connectors")
    public static class ConnectorResource {
        @GET
        public String getConnectors() {
            return CONNECTORS;
        }

        @GET
        @Path("cluster_mysql_test-test_database-test_table")
        public String getConnectorConfig1() {
            return CONNECTOR_CONFIG1;
        }
    }

    @Override
    protected Application configure() {
        return new ResourceConfig(ConnectorResource.class);
    }

    private TestingServer testingServer;

    private EmbeddedKafkaCluster kafka;

    private DB db;

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

        // start kafka https://mguenther.github.io/kafka-junit/
        kafka = provisionWith(EmbeddedKafkaClusterConfig.useDefaults());
        kafka.start();

        DBConfigurationBuilder configBuilder = DBConfigurationBuilder.newBuilder();
        configBuilder.setPort(3306);
        configBuilder.setDataDir("/tmp/mysql/test_database");
        db = DB.newEmbeddedDB(configBuilder.build());
        db.start();
        db.createDB("test_database");
        db.source("sql/test_table.sql", "test_database");
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

        DebeziumConfigBuilderPrototype dbConfigBuilderPrototype = new DebeziumConfigBuilderPrototype();
        dbConfigBuilderPrototype.addDebeziumConfigBuilder(DBType.MySQL, new MySQLDebeziumConfigBuilder());

        MasterDebeziumService masterDebeziumService = new MasterDebeziumService(zkConnector,
                dbTopicTaskPrototype, dbConfigBuilderPrototype, true);

        // -------------------- add a topic to zookeeper --------------------
        TopicConfig topicConfig = new TopicConfig();
        topicConfig.setDbType(DBType.MySQL);
        topicConfig.setKafkaConnectUrl("http://localhost:9998");

        EmbeddedKafkaClusterConfig.useDefaults().getKafkaConfig().getBrokerProperties()
                .forEach((k, v) -> topicConfig.addKafkaConfig(String.valueOf(k), String.valueOf(v)));

        topicConfig.addKafkaConfig(BOOTSTRAP_SERVERS_CONFIG, kafka.getBrokerList());
        topicConfig.addKafkaConfig(KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        topicConfig.addKafkaConfig(VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        topicConfig.addKafkaConfig(GROUP_ID_CONFIG, "test_group");
        topicConfig.addKafkaConfig(AUTO_OFFSET_RESET_CONFIG, "earliest");

        String topicConfigStr = JsonUtils.writeValueAsString(topicConfig);
        zkConnector.createNode(String.format("%s/cluster_mysql_test", masterDebeziumService.getTopicPath()), topicConfigStr);

        // -------------------- start master --------------------
        MasterService master = new MasterService(zkConnector, masterDebeziumService);
        master.startMaster();

        // -------------------- start slave --------------------
        SlaveTaskPrototype slaveTaskPrototype = new SlaveTaskPrototype();
        slaveTaskPrototype.addSlaveTask(DBType.MySQL, new MySQLSlaveTask());
        SlaveService slave = new SlaveService(new ZookeeperConnector(zkConfig, true),
                new SlaveZkService(masterDebeziumService.getTopicPath(), slaveTaskPrototype));
        slave.startSlave();

        // send topic a data
        MySQLSchemaChange schemaChange1 = new MySQLSchemaChange();
        schemaChange1.setDatabaseName("test_database");
        schemaChange1.setDdl("create table test_table (id int(11), name varchar(50))");
        schemaChange1.getSource().setServer("cluster_mysql_test");
        schemaChange1.getPosition().setFile("mysql-bin.000003");
        schemaChange1.getPosition().setPos(4132448L);
        schemaChange1.getPosition().setServerId("1");
        schemaChange1.getPosition().setTsSec(System.currentTimeMillis());
        KeyValue<String, String> record1 = new KeyValue<>("test_database", JsonUtils.writeValueAsString(schemaChange1));

        MySQLSchemaChange schemaChange2 = new MySQLSchemaChange();
        schemaChange2.setDatabaseName("test_database");
        schemaChange2.setDdl("ALTER TABLE test_database.test_table ADD test_a CHAR(1) DEFAULT 'a'");
        schemaChange2.getSource().setServer("cluster_mysql_test");
        schemaChange2.getPosition().setFile("mysql-bin.000003");
        schemaChange2.getPosition().setPos(4132449L);
        schemaChange2.getPosition().setServerId("1");
        schemaChange2.getPosition().setTsSec(System.currentTimeMillis());
        KeyValue<String, String> record2 = new KeyValue<>("test_database", JsonUtils.writeValueAsString(schemaChange2));

        kafka.send(to("cluster_mysql_test", Arrays.asList(record1, record2)).build());
        kafka.observe(on("cluster_mysql_test", 2).build());

        Thread.sleep(TimeUnit.SECONDS.toMillis(30));
    }

}
