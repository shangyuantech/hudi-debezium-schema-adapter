package org.apache.hudi.debezium.mysql.jdbc;

import com.mysql.cj.jdbc.MysqlDataSource;
import io.debezium.connector.mysql.MySqlValueConverters;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.MapListHandler;
import org.apache.commons.lang.StringUtils;
import org.apache.hudi.debezium.util.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.sql.*;
import java.util.Calendar;
import java.util.List;

public class JDBCUtils {

    private final static Logger logger = LoggerFactory.getLogger(JDBCUtils.class);

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

    public static boolean checkSsl(String databaseSslMode) {
        boolean ssl = false;
        switch (databaseSslMode) {
            case "verify_ca":
            case "verify_identity":
                ssl = true;
                break;
            case "disabled":
            case "preferred":
            case "required":
            default:
                break;
        }

        return ssl;
    }

    private static final String SQL_PARTITION =
            "select table_schema, table_name, partition_name, subpartition_name, " +
                    "partition_ordinal_position, subpartition_ordinal_position, " +
                    "partition_method, subpartition_method, partition_expression, subpartition_expression, " +
                    "partition_description " +
            "from information_schema.partitions " +
            "where table_schema = schema() " +
            "and table_name = ? order by partition_ordinal_position";

    public static List<Partition> getPartitions(Connection connection, String tableName) throws SQLException {
         return JsonUtils.transformList(
                 queryRunner.query(connection, SQL_PARTITION, new MapListHandler(), tableName),
                 Partition.class);
    }

    public static Object readField(ResultSet rs, int fieldNo, Column actualColumn, Table actualTable) throws SQLException {
        if (actualColumn.jdbcType() == Types.TIME) {
            return readTimeField(rs, fieldNo);
        }
        else if (actualColumn.jdbcType() == Types.DATE) {
            return readDateField(rs, fieldNo, actualColumn, actualTable);
        }
        // This is for DATETIME columns (a logical date + time without time zone)
        // by reading them with a calendar based on the default time zone, we make sure that the value
        // is constructed correctly using the database's (or connection's) time zone
        else if (actualColumn.jdbcType() == Types.TIMESTAMP) {
            return readTimestampField(rs, fieldNo, actualColumn, actualTable);
        }
        // JDBC's rs.GetObject() will return a Boolean for all TINYINT(1) columns.
        // TINYINT columns are reprtoed as SMALLINT by JDBC driver
        else if (actualColumn.jdbcType() == Types.TINYINT || actualColumn.jdbcType() == Types.SMALLINT) {
            // It seems that rs.wasNull() returns false when default value is set and NULL is inserted
            // We thus need to use getObject() to identify if the value was provided and if yes then
            // read it again to get correct scale
            return rs.getObject(fieldNo) == null ? null : rs.getInt(fieldNo);
        }
        else {
            return rs.getObject(fieldNo);
        }
    }

    private static Object readTimeField(ResultSet rs, int fieldNo) throws SQLException {
        Blob b = rs.getBlob(fieldNo);
        if (b == null) {
            return null; // Don't continue parsing time field if it is null
        }

        try {
            return MySqlValueConverters.stringToDuration(new String(b.getBytes(1, (int) (b.length())), "UTF-8"));
        }
        catch (UnsupportedEncodingException e) {
            logger.error("Could not read MySQL TIME value as UTF-8");
            throw new RuntimeException(e);
        }
    }

    private static Object readDateField(ResultSet rs, int fieldNo, Column column, Table table) throws SQLException {
        Blob b = rs.getBlob(fieldNo);
        if (b == null) {
            return null; // Don't continue parsing date field if it is null
        }

        try {
            return MySqlValueConverters.stringToLocalDate(new String(b.getBytes(1, (int) (b.length())), "UTF-8"), column, table);
        }
        catch (UnsupportedEncodingException e) {
            logger.error("Could not read MySQL TIME value as UTF-8");
            throw new RuntimeException(e);
        }
    }

    private static Object readTimestampField(ResultSet rs, int fieldNo, Column column, Table table) throws SQLException {
        Blob b = rs.getBlob(fieldNo);
        if (b == null) {
            return null; // Don't continue parsing timestamp field if it is null
        }

        try {
            return MySqlValueConverters.containsZeroValuesInDatePart((new String(b.getBytes(1, (int) (b.length())), "UTF-8")), column, table) ? null
                    : rs.getTimestamp(fieldNo, Calendar.getInstance());
        }
        catch (UnsupportedEncodingException e) {
            logger.error("Could not read MySQL TIME value as UTF-8");
            throw new RuntimeException(e);
        }
    }

}
