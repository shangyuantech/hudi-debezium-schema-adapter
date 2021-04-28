package io.debezium.connector.mysql;

import io.debezium.config.Configuration;
import org.apache.hudi.debezium.kafka.consumer.record.SchemaRecord;
import org.apache.hudi.debezium.mysql.data.MySQLSchemaChange;
import org.apache.hudi.debezium.zookeeper.task.AlterField;
import org.apache.hudi.schema.common.DDLType;
import org.apache.hudi.schema.ddl.DDLStat;
import org.apache.hudi.schema.parser.DefaultSchemaParser;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;
import java.util.function.Consumer;

public class MySQLRecordProcessor {

    private final static DefaultSchemaParser defaultSqlParser = new DefaultSchemaParser();

    private final MySqlTaskContext context;

    private final MySQLReader reader;

    public MySQLRecordProcessor(String database, String table, Configuration config, String sql,
                                MySQLSchemaChange schemaRecord) {
        this.context = createAndStartTaskContext(config, new Filters.Builder(config).build(), schemaRecord);
        this.reader = new MySQLReader(database, table, sql, context,
                defaultSqlParser.getSqlStat(schemaRecord.getDdl()));
    }

    private MySqlTaskContext createAndStartTaskContext(Configuration config, Filters filters,
                                                       MySQLSchemaChange schemaRecord) {

        MySqlTaskContext taskContext = new MySqlTaskContext(config, filters);
        taskContext.start();

        taskContext.source().setBinlogStartPoint(schemaRecord.getPosition().getFile(),
                schemaRecord.getPosition().getPos());
        return taskContext;
    }

    public void startTask() {
        this.reader.start();
    }

    public void pollData(Consumer<List<SourceRecord>> consumer) throws InterruptedException {
        List<SourceRecord> records;
        while ((records = this.reader.poll()) != null) {
            consumer.accept(records);
        }
    }

    public void stopTask() {
        this.context.shutdown();
        this.reader.stop();
    }
}
