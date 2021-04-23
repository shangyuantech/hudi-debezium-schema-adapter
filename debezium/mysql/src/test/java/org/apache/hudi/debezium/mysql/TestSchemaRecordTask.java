package org.apache.hudi.debezium.mysql;

import ch.vorburger.mariadb4j.DBConfigurationBuilder;
import ch.vorburger.mariadb4j.junit.MariaDB4jRule;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;
import com.alibaba.druid.util.JdbcConstants;
import org.apache.hudi.debezium.common.DBType;
import org.apache.hudi.debezium.common.TopicConfig;
import org.apache.hudi.debezium.mysql.data.MySQLSchemaChange;
import org.apache.hudi.debezium.mysql.impl.master.MySQLRecordService;
import org.apache.hudi.debezium.zookeeper.task.Task;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Application;

import java.util.List;
import java.util.Optional;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;

public class TestSchemaRecordTask extends JerseyTest {


    private final static String topic = "cluster_mysql_test_database";

    private final static String database = "test_database";

    public static final String CONNECTORS = String.join(",",
            String.format("\"%s-%s-%s\"", topic, database, "test_table_partition_list"));

    @Path("connectors")
    public static class ConnectorResource {
        @GET
        public String getConnectors() {
            return "[" + CONNECTORS + "]";
        }

        private String getConfig(String table) {
            return "{\n" +
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
                    "\t\t\"database.whitelist\": \"" + table + "\",\n" +
                    "\t\t\"include.schema.changes\": \"true\",\n" +
                    "\t\t\"name\": \"" + topic + "-" + database + table + "\",\n" +
                    "\t\t\"table.whitelist\": \"" + database + "." + table + "\",\n" +
                    "\t\t\"time.precision.mode\": \"connect\",\n" +
                    "\t\t\"tombstones.on.delete\": \"false\"\n" +
                    "\t},\n" +
                    "\t\"name\": \"" + topic + "-" + database + table + "\",\n" +
                    "\t\"tasks\": [\n" +
                    "\t\t{\n" +
                    "\t\t\t\"connector\": \"" + topic + "-" + database + table + "\",\n" +
                    "\t\t\t\"task\": 0\n" +
                    "\t\t}\n" +
                    "\t],\n" +
                    "\t\"type\": \"source\"\n" +
                    "}";
        }


        @GET
        @Path(topic + "-" + database + "-test_table_partition_list")
        public String getConnectorConfig1() {
            return getConfig("test_table_partition_list");
        }
    }

    @Override
    protected Application configure() {
        return new ResourceConfig(TestMySQLClusterService.ConnectorResource.class);
    }

    DBConfigurationBuilder configBuilder = DBConfigurationBuilder.newBuilder()
            .setPort(3306)
            .setDataDir("/tmp/mysql/test_database");

    @Rule
    public MariaDB4jRule dbRule = new MariaDB4jRule(
            configBuilder.build(),
            "test_database",
            "sql/test_table.sql"
    );

    TopicConfig topicConfig = new TopicConfig().setDbType(DBType.MySQL)
            .addKafkaConfig(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
            .setKafkaConnectUrl("http://localhost:9998");

    private MySQLSchemaChange getSchemaRecord(String ddl) {
        MySQLSchemaChange schemaRecord = new MySQLSchemaChange();
        schemaRecord.setDatabaseName("test_database");
        schemaRecord.setDdl(ddl);

        MySQLSchemaChange.Position position = new MySQLSchemaChange.Position();
        position.setFile("mysql-bin.000003");
        position.setPos(4132448L);
        position.setTsSec(System.currentTimeMillis());
        position.setServerId("1");
        schemaRecord.setPosition(position);

        schemaRecord.setSource(new MySQLSchemaChange.Source().setServer(topic));

        return schemaRecord;
    }

    @Test
    public void testPartitionList() throws Exception {
        MySQLRecordService recordService = new MySQLRecordService(topic, topicConfig, StringDeserializer.class);
        Optional<Task<?>> task = recordService.publishTask(getSchemaRecord(
                "ALTER TABLE test_database.test_table_partition_list ADD test_a CHAR(1) DEFAULT 'a'"));

        Assert.assertTrue(task.isPresent());
        System.out.println(task.get());

        Assert.assertEquals(task.get().getTasks().size(), 3);

        // sql
        List<SQLStatement> stmtList = SQLUtils.parseStatements(task.get().getTasks().get(0).getSql(), JdbcConstants.MYSQL);
        List<SQLObject> where = ((MySqlSelectQueryBlock)((SQLSelectStatement) stmtList.get(0))
                .getSelect().getQuery())
                .getWhere()
                .getChildren();
        Assert.assertEquals(where.size(), 3);
        Assert.assertEquals(((SQLCharExpr) where.get(1)).getText(), "10");
        Assert.assertEquals(((SQLCharExpr) where.get(2)).getText(), "20");
    }

    @Test
    public void testPartitionHash() throws Exception {
        MySQLRecordService recordService = new MySQLRecordService(topic, topicConfig, StringDeserializer.class);
        Optional<Task<?>> task = recordService.publishTask(getSchemaRecord(
                "ALTER TABLE test_database.test_table_partition_hash ADD test_a CHAR(1) DEFAULT 'a'"));

        Assert.assertTrue(task.isPresent());
        System.out.println(task.get());

        Assert.assertEquals(task.get().getTasks().size(), 4);

        // sql
        List<SQLStatement> stmtList = SQLUtils.parseStatements(task.get().getTasks().get(0).getSql(), JdbcConstants.MYSQL);
        List<SQLObject> where = ((MySqlSelectQueryBlock)((SQLSelectStatement) stmtList.get(0))
                .getSelect().getQuery())
                .getWhere()
                .getChildren();
        Assert.assertEquals(where.size(), 2);
        Assert.assertEquals(((SQLMethodInvokeExpr)where.get(0)).getMethodName(), "mod");
        // year(`birthdate`)
        Assert.assertEquals(((SQLMethodInvokeExpr)where.get(0)).getArguments().get(0).toString(),
                task.get().getTasks().get(0).getPartitionFields().get(0));
        Assert.assertEquals(((SQLIntegerExpr) where.get(1)).getValue(), 0);
    }

}
