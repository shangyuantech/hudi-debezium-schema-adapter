package org.apache.hudi.debezium.kafka.config;

import java.util.HashMap;
import java.util.Set;

public enum DebeziumConfigCache {

    CACHE;

    private final HashMap<String, DebeziumConfig> defaultConfigs = new HashMap<>();

    public void addDebeziumConfig(String serverName, DebeziumConfig debeziumConfig) {
        if (!debeziumConfig.equals(defaultConfigs.get(serverName)))
            defaultConfigs.put(serverName, debeziumConfig);
    }

    public DebeziumConfig getDefaultConfig(String serverName) {
        return defaultConfigs.get(serverName);
    }

    public void removeDefaultConfig(String serverName) {
        defaultConfigs.remove(serverName);
    }

    public Set<String> getCacheServiceNames() {
        return defaultConfigs.keySet();
    }
}
