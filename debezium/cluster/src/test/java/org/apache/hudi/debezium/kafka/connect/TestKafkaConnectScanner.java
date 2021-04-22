package org.apache.hudi.debezium.kafka.connect;

import org.apache.hudi.debezium.common.DBType;
import org.apache.hudi.debezium.common.TopicConfig;
import org.apache.hudi.debezium.example.TestDebeziumConfigBuilder;
import org.apache.hudi.debezium.config.DebeziumConfigCache;
import org.apache.hudi.debezium.kafka.connect.scanner.ConnectorScannerTask;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.Assert;
import org.junit.Test;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Application;

import java.util.concurrent.TimeUnit;

import static org.apache.hudi.debezium.kafka.connect.TestKafkaConnectUtils.*;
import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;

public class TestKafkaConnectScanner extends JerseyTest {

    @Path("connectors")
    public static class ConnectorResource {
        @GET
        public String getConnectors() {
            return CONNECTORS;
        }

        @GET
        @Path("cluster_mysql_test-test_database-test_table1")
        public String getConnectorConfig1() {
            return CONNECTOR_CONFIG1;
        }

        @GET
        @Path("cluster_mysql_test-test_database-test_table2")
        public String getConnectorConfig2() {
            return CONNECTOR_CONFIG2;
        }
    }

    @Override
    protected Application configure() {
        return new ResourceConfig(TestKafkaConnectUtils.ConnectorResource.class);
    }

    private final String kafkaConnectUrl = "http://localhost:9998";

    @Test
    public void testScanner() throws InterruptedException {
        DebeziumConfigBuilderPrototype configBuilderPrototype = new DebeziumConfigBuilderPrototype();
        configBuilderPrototype.addDebeziumConfigBuilder(DBType.MySQL, new TestDebeziumConfigBuilder());

        TopicConfig topicConfig = new TopicConfig();
        topicConfig.setDbType(DBType.MySQL);
        topicConfig.addKafkaConfig(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        topicConfig.setKafkaConnectUrl(kafkaConnectUrl);

        ConnectorScannerTask scanner = new ConnectorScannerTask(configBuilderPrototype);
        scanner.addConnector("cluster_mysql_test", topicConfig);
        scanner.start();

        Thread.sleep(TimeUnit.SECONDS.toMillis(25));

        Assert.assertEquals(DebeziumConfigCache.CACHE.getCacheServiceNames().size(), 1);

        scanner.removeConnector("cluster_mysql_test");
        Thread.sleep(TimeUnit.SECONDS.toMillis(10));

        scanner.stopTask();
        Thread.sleep(TimeUnit.SECONDS.toMillis(15));
    }

}
