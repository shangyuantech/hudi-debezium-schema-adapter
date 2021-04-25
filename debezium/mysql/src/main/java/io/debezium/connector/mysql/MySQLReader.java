package io.debezium.connector.mysql;

import io.debezium.connector.SnapshotRecord;
import io.debezium.data.Envelope;
import io.debezium.relational.TableId;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.Map;
import java.util.Set;

public class MySQLReader extends AbstractReader {

    public MySQLReader(String name, MySqlTaskContext context) {
        super(name, context, null);
    }

    @Override
    protected void doStart() {
        System.out.println("doStart");
    }

    @Override
    protected void doStop() {
        System.out.println("doStop");
    }

    @Override
    protected void doCleanup() {
        System.out.println("doCleanup");
    }

    public void enqueueRecord(SourceRecord record) throws InterruptedException {
        // todo
        //System.out.println(record);
        super.enqueueRecord(record);
    }

    public void enqueueSchemaChanges(String dbName, Set<TableId> tables, String ddlStatement) {
        // todo
    }

    protected SourceRecord replaceOffsetAndSource(SourceRecord record) {
        if (record == null) {
            return null;
        }
        Map<String, ?> newOffset = context.source().offset();
        final Struct envelope = (Struct) record.value();
        final Struct source = (Struct) envelope.get(Envelope.FieldName.SOURCE);
        if (SnapshotRecord.fromSource(source) == SnapshotRecord.TRUE) {
            SnapshotRecord.LAST.toSource(source);
        }
        return new SourceRecord(record.sourcePartition(),
                newOffset,
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                record.valueSchema(),
                record.value());
    }
}
