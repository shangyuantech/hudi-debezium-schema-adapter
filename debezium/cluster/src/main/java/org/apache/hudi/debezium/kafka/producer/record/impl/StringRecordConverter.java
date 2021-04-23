package org.apache.hudi.debezium.kafka.producer.record.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.hudi.debezium.kafka.producer.record.RecordConverter;
import org.apache.hudi.debezium.util.JsonUtils;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Map;

public class StringRecordConverter extends RecordConverter<String> {

    public StringRecordConverter(String serverName, String database, String table) {
        super(serverName, database, table);
    }

    @Override
    public StringSerializer getSerializer() {
        return new StringSerializer();
    }

    @Override
    public String getValue(Map<String, Object> value, Map<String, String> type) throws JsonProcessingException {
        return JsonUtils.writeValueAsString(value);
    }

    @Override
    public String getKey(Object key) throws JsonProcessingException {
        return JsonUtils.writeValueAsString(key);
    }
}
