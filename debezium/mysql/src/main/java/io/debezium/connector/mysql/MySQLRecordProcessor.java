package io.debezium.connector.mysql;

import io.debezium.config.Configuration;
import org.apache.hudi.debezium.zookeeper.task.AlterField;
import org.apache.hudi.schema.common.DDLType;
import org.apache.hudi.schema.ddl.DDLStat;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;
import java.util.function.Consumer;

public class MySQLRecordProcessor {

    private final MySqlTaskContext context;

    private final MySQLReader reader;

    public MySQLRecordProcessor(String database, String table, Configuration config,
                                DDLStat ddlStat) {
        this.context = createAndStartTaskContext(config, new Filters.Builder(config).build());
        this.reader = new MySQLReader(database, table, context, ddlStat);
    }

    private MySqlTaskContext createAndStartTaskContext(Configuration config,
                                                              Filters filters) {
        MySqlTaskContext taskContext = new MySqlTaskContext(config, filters);
        taskContext.start();
        taskContext.source().setBinlogStartPoint("", 0L);
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
}
