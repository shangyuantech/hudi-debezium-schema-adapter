package org.apache.hudi.debezium.kafka.config;

import java.util.Map;

public abstract class DebeziumConfig {

    public DebeziumConfig(String serverName) {
        this.serverName = serverName;
    }

    private String serverName;

    public String getServerName() {
        return serverName;
    }

    public void setServerName(String serverName) {
        this.serverName = serverName;
    }

    abstract public void init(Map<String, String> configMap);

    abstract public boolean equals(Object o);

    @Override
    public String toString() {
        return "DebeziumConfig{" +
                "serverName='" + serverName + '\'' +
                '}';
    }
}
