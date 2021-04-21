package org.apache.hudi.debezium.mysql.impl.connect;

import org.apache.hudi.debezium.common.DBType;
import org.apache.hudi.debezium.kafka.config.DebeziumConfig;
import org.apache.hudi.debezium.kafka.connect.IDebeziumConfigBuilder;
import org.apache.hudi.debezium.mysql.data.MySQLDebeziumConfig;

import java.util.Map;

import static org.apache.hudi.debezium.common.DBType.MySQL;

public class MySQLDebeziumConfigBuilder implements IDebeziumConfigBuilder {

    @Override
    public DBType getDbType() {
        return MySQL;
    }

    @Override
    public DebeziumConfig buildConfig(Map<String, String> config) {
        MySQLDebeziumConfig mysqlConfig = new MySQLDebeziumConfig(config.get(debeziumPropServerName));
        mysqlConfig.init(config);
        return mysqlConfig;
    }
}
