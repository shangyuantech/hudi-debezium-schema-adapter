package io.debezium.connector.mysql;

import io.debezium.config.Configuration;
import io.debezium.function.BufferedBlockingConsumer;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import org.apache.kafka.connect.source.SourceRecord;

import java.sql.SQLException;
import java.util.Map;

public class MySQLRecordProcessor {

    private final MySqlTaskContext context;
    private final BufferedBlockingConsumer<SourceRecord> bufferedRecordQueue;
    private final MySQLReader mySqlReader;

    public MySQLRecordProcessor(Configuration config) {
        this.context = new MySqlTaskContext(config, new Filters.Builder(config).build());
        this.mySqlReader = new MySQLReader("snapshot", context);
        this.context.start();
        this.mySqlReader.start();

        this.bufferedRecordQueue = BufferedBlockingConsumer.bufferLast(mySqlReader::enqueueRecord);
        //this.mySqlTaskContext.makeRecord().regenerate();

        // set source for empty
        context.source().setBinlogStartPoint("mysql-bin.00001", 0);
    }

    public MySQLReader getReader() {
        return mySqlReader;
    }

    public MySqlTaskContext getContext() {
        return context;
    }

    public MySqlSchema getDbSchema() {
        return context.dbSchema();
    }

    public RecordMakers.RecordsForTable getMaker(TableId tableId) throws SQLException {
        final MySqlSchema schema = getDbSchema();

        getJdbcConnection().query("SHOW CREATE TABLE " + quote(tableId), rs -> {
            if (rs.next()) {
                schema.applyDdl(context.source(), tableId.catalog(), rs.getString(2),
                        mySqlReader::enqueueSchemaChanges);
            }
        });

        RecordMakers recordMakers = context.makeRecord();
        //recordMakers.regenerate();
        recordMakers.assign(0, tableId);

        return recordMakers.forTable(tableId, null, bufferedRecordQueue);
    }

    public JdbcConnection getJdbcConnection() {
        return context.getConnectionContext().jdbc();
    }

    private String quote(String dbOrTableName) {
        return "`" + dbOrTableName + "`";
    }

    private String quote(TableId id) {
        return quote(id.catalog()) + "." + quote(id.table());
    }

    public String getSelect(TableId tableId) {
        Map<TableId, String> selectOverrides = context.getConnectorConfig().getSnapshotSelectOverridesByTable();
        return selectOverrides.getOrDefault(tableId, "SELECT * FROM " + quote(tableId));
    }

    public void finishRecord() throws InterruptedException {
        this.bufferedRecordQueue.close(mySqlReader::replaceOffsetAndSource);
    }
}
