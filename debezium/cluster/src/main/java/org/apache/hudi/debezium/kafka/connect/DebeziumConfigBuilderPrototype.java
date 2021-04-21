package org.apache.hudi.debezium.kafka.connect;

import org.apache.hudi.debezium.common.DBType;
import org.apache.hudi.debezium.common.exception.DebeziumConfigBuilderNotFoundException;

import java.util.HashMap;

public class DebeziumConfigBuilderPrototype {

    public DebeziumConfigBuilderPrototype() {
    }

    private final HashMap<DBType, IDebeziumConfigBuilder> debeziumConfigBuilderPrototype = new HashMap<>();

    public DebeziumConfigBuilderPrototype addDebeziumConfigBuilder(DBType dbType, IDebeziumConfigBuilder debeziumConfigBuilder) {
        debeziumConfigBuilderPrototype.put(dbType, debeziumConfigBuilder);
        return this;
    }

    public IDebeziumConfigBuilder getDebeziumConfigBuilder(DBType dbType) throws DebeziumConfigBuilderNotFoundException {
        if (debeziumConfigBuilderPrototype.containsKey(dbType)) {
            return debeziumConfigBuilderPrototype.get(dbType);
        } else {
            throw new DebeziumConfigBuilderNotFoundException(
                    String.format("can not find debezium topic task instance , db_type = %s", dbType));
        }
    }

}
