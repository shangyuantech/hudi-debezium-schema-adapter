package org.apache.hudi.debezium.kafka.connect;

import org.apache.avro.Schema;
import org.apache.hudi.debezium.kafka.util.AvroUtils;
import org.apache.hudi.debezium.kafka.util.KafkaConnectUtils;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.glassfish.jersey.test.TestProperties;
import org.junit.Test;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Application;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TestKafkaConnectUtils extends JerseyTest {

    public static final String CONNECTORS = "[\"cluster_mysql_test-test_database-test_table1\",\"cluster_mysql_test-test_database-test_table2\"]";

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
            "\t\t\"name\": \"cluster_mysql_test-test_database-test_table1\",\n" +
            "\t\t\"table.whitelist\": \"test_database.test_table1\",\n" +
            "\t\t\"time.precision.mode\": \"connect\",\n" +
            "\t\t\"tombstones.on.delete\": \"false\"\n" +
            "\t},\n" +
            "\t\"name\": \"cluster_mysql_test-test_database-test_table1\",\n" +
            "\t\"tasks\": [\n" +
            "\t\t{\n" +
            "\t\t\t\"connector\": \"cluster_mysql_test-test_database-test_table1\",\n" +
            "\t\t\t\"task\": 0\n" +
            "\t\t}\n" +
            "\t],\n" +
            "\t\"type\": \"source\"\n" +
            "}";

    public static final String CONNECTOR_CONFIG2 = "{\n" +
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
            "\t\t\"name\": \"cluster_mysql_test-test_database-test_table2\",\n" +
            "\t\t\"table.whitelist\": \"test_database.test_table2\",\n" +
            "\t\t\"time.precision.mode\": \"connect\",\n" +
            "\t\t\"tombstones.on.delete\": \"false\"\n" +
            "\t},\n" +
            "\t\"name\": \"cluster_mysql_test-test_database-test_table2\",\n" +
            "\t\"tasks\": [\n" +
            "\t\t{\n" +
            "\t\t\t\"connector\": \"cluster_mysql_test-test_database-test_table2\",\n" +
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

    @Path("subjects")
    public static class SchemaRegistryResource {

        @GET
        @Path("/cluster_mysql_test.test_database.test-value/versions/latest")
        public String getSchema() throws IOException {
            Schema avroSchema = AvroUtils.getAvroSchemaByFile("src/test/resources/avro/test-value.avro");
            return avroSchema.toString(true);
        }
    }

    @Override
    protected Application configure() {
        return new ResourceConfig(ConnectorResource.class, SchemaRegistryResource.class);
    }

    private final String kafkaConnectUrl = "http://localhost:9998";

    @Test
    public void test() {
        final String connectors = target("connectors").request().get(String.class);
        assertEquals(CONNECTORS, connectors);
    }

    @Test
    public void testGetConnectors() throws IOException {
        List<String> connectors = KafkaConnectUtils.getConnectors(kafkaConnectUrl);
        assertEquals(connectors.size(), 2);
        assertEquals("cluster_mysql_test-test_database-test_table1", connectors.get(0));
    }

    @Test
    public void testGetConnectorConfig() throws IOException {
        Map<String, String> connectorConfig = KafkaConnectUtils.getConnectorConfig(kafkaConnectUrl,
                "cluster_mysql_test-test_database-test_table1");
        assertEquals(connectorConfig.get("database.server.name"), "cluster_mysql_test");
    }

    @Test
    public void testSchemaRegistry() throws IOException {
        Schema schema = KafkaConnectUtils.getAvroSchema("http://localhost:9998",
                "cluster_mysql_test.test_database.test-value");
        System.out.println(schema.toString(true));
    }

}
