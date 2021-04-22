package org.apache.hudi.debezium.kafka.connect;

import org.apache.hudi.debezium.common.DBType;
import org.apache.hudi.debezium.config.DebeziumConfig;

import java.util.Map;

public interface IDebeziumConfigBuilder {

    String debeziumPropServerName = "database.server.name";

    DBType getDbType();

    DebeziumConfig buildConfig(Map<String, String> config);
}
