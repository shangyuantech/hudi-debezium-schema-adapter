package org.apache.hudi.debezium.mysql;

import ch.vorburger.mariadb4j.DBConfigurationBuilder;
import ch.vorburger.mariadb4j.junit.MariaDB4jRule;
import org.apache.hudi.debezium.mysql.impl.jdbc.JDBCUtils;
import org.apache.hudi.debezium.mysql.impl.jdbc.Partition;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

public class TestJDBCUtils {

    DBConfigurationBuilder configBuilder = DBConfigurationBuilder.newBuilder()
            .setPort(3306)
            .setDataDir("/tmp/mysql/test_database");

    @Rule
    // https://github.com/vorburger/MariaDB4j
    public MariaDB4jRule dbRule = new MariaDB4jRule(
            configBuilder.build(),
            "test_database",
            "sql/test_table.sql"
    );

    private Connection getConnection() throws SQLException {
        return JDBCUtils.createConnection("localhost", "3306", "root", "",
                "test_database", false);
    }

    @Test
    public void testGetConnection() throws SQLException {
        System.out.println(dbRule.getURL());
        Connection conn = getConnection();
        conn.close();
    }

    @Test
    public void testPartition() throws SQLException {
        Connection conn = getConnection();
        List<Partition> partitions = JDBCUtils.getPartitions(conn, "test_table");

        System.out.println(partitions);
        Assert.assertEquals(partitions.size(), 3);
    }

}
