package org.apache.hudi.debezium.mysql;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.hudi.debezium.mysql.data.MySQLSchemaChange;
import org.apache.hudi.debezium.util.JsonUtils;
import org.junit.Assert;
import org.junit.Test;

public class TestSchemaChange {

    private String data = "{\n" +
            "  \"source\" : {\n" +
            "    \"server\" : \"cluster_mysql_bigdata\"\n" +
            "  },\n" +
            "  \"position\" : {\n" +
            "    \"ts_sec\" : 1618562630,\n" +
            "    \"file\" : \"mysql-bin.000003\",\n" +
            "    \"pos\" : 4132448,\n" +
            "    \"server_id\" : 1\n" +
            "  },\n" +
            "  \"databaseName\" : \"bigdata\",\n" +
            "  \"ddl\" : \" create table test (id int(11), name varchar(50))\"\n" +
            "}";

    @Test
    public void testSchemaChange () throws JsonProcessingException {
        MySQLSchemaChange schemaChange = JsonUtils.readValue(data, MySQLSchemaChange.class);

        Assert.assertEquals(schemaChange.getDatabaseName(), "bigdata");
        Assert.assertEquals(schemaChange.getSource().getServer(), "cluster_mysql_bigdata");
        Assert.assertEquals(schemaChange.getPosition().getTsSec().longValue(), 1618562630L);
    }

}
