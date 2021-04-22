package org.apache.hudi.debezium.mysql.impl.jdbc;

import com.mysql.cj.jdbc.MysqlDataSource;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.MapListHandler;
import org.apache.commons.lang.StringUtils;
import org.apache.hudi.debezium.util.JsonUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

public class JDBCUtils {

    private static final QueryRunner queryRunner = new QueryRunner();

    public static Connection createConnection(String hostName, String port, String username, String password,
                                              String database, Boolean ssl) throws SQLException {
        MysqlDataSource mysqlDS = new MysqlDataSource();
        //mysqlDS.setLogger("com.mysql.jdbc.log.StandardLogger");
        mysqlDS.setServerName(hostName);
        mysqlDS.setPort(Integer.parseInt(port));
        mysqlDS.setUser(username);
        mysqlDS.setPassword(password);
        if (StringUtils.isNotBlank(database)) mysqlDS.setDatabaseName(database);
        mysqlDS.setUseSSL(ssl);
        mysqlDS.setRequireSSL(false);

        return mysqlDS.getConnection();
    }

    private static final String SQL_PARTITION =
            "select table_schema, table_name, partition_name, subpartition_name, " +
                    "partition_ordinal_position, subpartition_ordinal_position, " +
                    "partition_method, subpartition_method, partition_expression, subpartition_expression, " +
                    "partition_description " +
            "from information_schema.partitions " +
            "where table_schema = schema() " +
            "and table_name = ?";

    public static List<Partition> getPartitions(Connection connection, String tableName) throws SQLException {
         return JsonUtils.transformList(
                 queryRunner.query(connection, SQL_PARTITION, new MapListHandler(), tableName),
                 Partition.class);
    }

}
