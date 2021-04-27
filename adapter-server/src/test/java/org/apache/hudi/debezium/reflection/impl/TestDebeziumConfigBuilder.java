package org.apache.hudi.debezium.reflection.impl;


import org.apache.hudi.debezium.common.DBType;
import org.apache.hudi.debezium.config.DebeziumConfig;
import org.apache.hudi.debezium.kafka.connect.IDebeziumConfigBuilder;

import java.util.Map;

import static org.apache.hudi.debezium.common.DBType.None;

public class TestDebeziumConfigBuilder implements IDebeziumConfigBuilder {

    @Override
    public DBType getDbType() {
        return None;
    }

    @Override
    public DebeziumConfig buildConfig(Map<String, String> config) {
        return null;
    }
}
