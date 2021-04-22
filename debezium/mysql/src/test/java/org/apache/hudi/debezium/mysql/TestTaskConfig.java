package org.apache.hudi.debezium.mysql;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.hudi.debezium.mysql.data.MySQLDebeziumConfig;
import org.apache.hudi.debezium.mysql.data.MySQLTask;
import org.apache.hudi.debezium.util.JsonUtils;
import org.apache.hudi.debezium.zookeeper.task.SubTask;
import org.apache.hudi.schema.ddl.DDLStat;
import org.apache.hudi.schema.parser.DefaultSchemaParser;
import org.junit.Assert;
import org.junit.Test;

public class TestTaskConfig {

    public static final MySQLDebeziumConfig mysqlConfig = new MySQLDebeziumConfig("cluster_bigdata_test")
            .setHostname("localhost")
            .setPort("3306")
            .setUser("test")
            .setPassword("test");

    @Test
    public void testDebeConfig() throws JsonProcessingException {
        String configStr = JsonUtils.writeValueAsString(mysqlConfig);
        System.out.println(configStr);
        MySQLDebeziumConfig configCast = JsonUtils.readValue(configStr, MySQLDebeziumConfig.class);

        Assert.assertEquals(configCast.getServerName(), mysqlConfig.getServerName());
        Assert.assertEquals(configCast.getHostname(), mysqlConfig.getHostname());
        Assert.assertEquals(configCast.getPort(), mysqlConfig.getPort());
        Assert.assertEquals(configCast.getUser(), mysqlConfig.getUser());
        Assert.assertEquals(configCast.getPassword(), mysqlConfig.getPassword());
    }

    @Test
    public void testTasks() throws JsonProcessingException {
        DefaultSchemaParser schemaParser = new DefaultSchemaParser();
        DDLStat ddlStat = schemaParser.getSqlStat("ALTER TABLE test_database.test_table ADD test_a CHAR(1) DEFAULT 'a'");

        MySQLTask task = new MySQLTask("test_task", mysqlConfig).setDdlType(ddlStat.getDdlType());
        SubTask subTask = new SubTask(String.format("%s#%s", "test_task", "task_1")).setSql("select * from test_table");
        task.addTask(subTask);

        String taskStr = JsonUtils.writeValueAsString(task);
        System.out.println(taskStr);
        MySQLTask taskCast = JsonUtils.readValue(taskStr, MySQLTask.class);

        Assert.assertEquals(taskCast.getName(), task.getName());
        Assert.assertEquals(taskCast.getTasks().size(), 1);
        Assert.assertEquals(taskCast.getTasks().get(0).getName(), subTask.getName());
    }

}
