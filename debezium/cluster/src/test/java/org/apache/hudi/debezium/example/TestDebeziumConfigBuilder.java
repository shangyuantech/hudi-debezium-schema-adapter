package org.apache.hudi.debezium.example;

import org.apache.hudi.debezium.common.DBType;
import org.apache.hudi.debezium.config.DebeziumConfig;
import org.apache.hudi.debezium.kafka.connect.IDebeziumConfigBuilder;

import java.util.Map;

import static org.apache.hudi.debezium.common.DBType.MySQL;

public class TestDebeziumConfigBuilder implements IDebeziumConfigBuilder {

    @Override
    public DBType getDbType() {
        return MySQL;
    }

    @Override
    public DebeziumConfig buildConfig(Map<String, String> config) {
        TestDebeziumConfig testDebeziumConfig = new TestDebeziumConfig(config.get(debeziumPropServerName));
        testDebeziumConfig.init(config);
        return testDebeziumConfig;
    }
}
