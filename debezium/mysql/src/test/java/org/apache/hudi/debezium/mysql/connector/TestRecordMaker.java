package org.apache.hudi.debezium.mysql.connector;

import ch.vorburger.mariadb4j.DBConfigurationBuilder;
import ch.vorburger.mariadb4j.junit.MariaDB4jRule;
import io.debezium.config.Configuration;
import io.debezium.connector.mysql.*;
import io.debezium.connector.mysql.MySQLRecordProcessor;
import io.debezium.connector.mysql.MySQLStatementFactory;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.history.FileDatabaseHistory;
import io.debezium.util.Clock;
import org.apache.hudi.debezium.mysql.jdbc.JDBCUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicLong;

public class TestRecordMaker {

    DBConfigurationBuilder configBuilder = DBConfigurationBuilder.newBuilder()
            .setPort(3307)
            .setDataDir("/tmp/mysql/test_database");

    @Rule
    public MariaDB4jRule dbRule = new MariaDB4jRule(
            configBuilder.build(),
            "test_database",
            "sql/test_table.sql"
    );

    /**
     * @return Configuration builder initialized with JDBC connection parameters.
     */
    public Configuration.Builder defaultJdbcConfigBuilder() {
        return Configuration.create()
                .with(MySqlConnectorConfig.HOSTNAME, System.getProperty("database.hostname", "localhost"))
                .with(MySqlConnectorConfig.PORT, System.getProperty("database.port", "3307"))
                .with(MySqlConnectorConfig.USER, "root")
                .with(MySqlConnectorConfig.PASSWORD, "")
                .with(MySqlConnectorConfig.SERVER_NAME, "cluster_mysql_test")
                .with(MySqlConnectorConfig.DATABASE_WHITELIST, "test_database")
                .with(MySqlConnectorConfig.TABLE_INCLUDE_LIST, "test_database.test_table")
                .with(MySqlConnectorConfig.DATABASE_HISTORY, FileDatabaseHistory.class)
                .with(FileDatabaseHistory.FILE_PATH, "/tmp/debezium/file-db-history-enum-column.txt");
    }

    @Test
    public void testRecordMakerReadRow() throws SQLException, InterruptedException {
        TableId tableId = new TableId("test_database", null, "test_table");

        MySQLRecordProcessor processor = new MySQLRecordProcessor(defaultJdbcConfigBuilder().build());
        Assert.assertNotNull(processor);

        RecordMakers.RecordsForTable recordMaker = processor.getMaker(tableId);
        Assert.assertNotNull(recordMaker);

        MySqlSchema dbSchema = processor.getDbSchema();
        AtomicLong rowNum = new AtomicLong();
        final Clock clock = processor.getContext().getClock();

        MySQLStatementFactory factory = new MySQLStatementFactory(
                processor.getContext().getConnectorConfig().getSnapshotFetchSize());

        processor.getJdbcConnection().query(
                processor.getSelect(tableId), factory, rs -> {
                    final Table table = dbSchema.tableFor(tableId);
                    final int numColumns = table.columns().size();
                    final Object[] row = new Object[numColumns];
                    while (rs.next()) {
                        for (int i = 0, j = 1; i != numColumns; ++i, ++j) {
                            Column actualColumn = table.columns().get(i);
                            row[i] = JDBCUtils.readField(rs, j, actualColumn, table);
                        }

                        try {
                            recordMaker.read(row, clock.currentTimeAsInstant());
                            rowNum.incrementAndGet();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }

                    try {
                        processor.finishRecord();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
        });

        System.out.println(processor.getReader().poll());
    }
}
