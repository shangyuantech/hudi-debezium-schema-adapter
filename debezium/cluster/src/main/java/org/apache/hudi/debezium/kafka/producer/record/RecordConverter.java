package org.apache.hudi.debezium.kafka.producer.record;

import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public abstract class RecordConverter<T> {

    private final String serverName;

    private final String database;

    private final String table;

    public RecordConverter(String serverName, String database, String table) {
        this.serverName = serverName;
        this.database = database;
        this.table = table;
    }

    public String getServerName() {
        return serverName;
    }

    public String getDatabase() {
        return database;
    }

    public String getTable() {
        return table;
    }

    abstract public Serializer<T> getSerializer();

    abstract public T getValue(Map<String, Object> value, Map<String, String> type) throws Exception;

    abstract public T getKey(Object key) throws Exception;
}
