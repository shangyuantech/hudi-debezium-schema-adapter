package org.apache.hudi.debezium.example;

import org.apache.hudi.debezium.kafka.config.DebeziumConfig;

import java.util.Map;

public class TestDebeziumConfig extends DebeziumConfig {

    private String hostname = "";

    public TestDebeziumConfig(String serverName) {
        super(serverName);
    }

    public String getHostname() {
        return hostname;
    }

    @Override
    public void init(Map<String, String> configMap) {
        this.hostname = configMap.get("database.hostname");
    }

    @Override
    public boolean equals(Object o) {
        if (o == null ) {
            return false;
        }

        if (o instanceof TestDebeziumConfig) {
            return this.hostname.equals(((TestDebeziumConfig) o).getHostname()) &&
                    this.getServerName().equals(((TestDebeziumConfig) o).getServerName());
        }

        return false;
    }

    @Override
    public String toString() {
        return "TestDebeziumConfig{" +
                "hostname='" + hostname + '\'' +
                "} " + super.toString();
    }
}
