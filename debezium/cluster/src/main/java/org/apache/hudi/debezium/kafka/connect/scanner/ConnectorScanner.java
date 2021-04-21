package org.apache.hudi.debezium.kafka.connect.scanner;

import org.apache.hudi.debezium.common.DBType;
import org.apache.hudi.debezium.common.exception.DebeziumConfigBuilderNotFoundException;
import org.apache.hudi.debezium.kafka.config.DebeziumConfig;
import org.apache.hudi.debezium.kafka.config.DebeziumConfigCache;
import org.apache.hudi.debezium.kafka.connect.DebeziumConfigBuilderPrototype;
import org.apache.hudi.debezium.kafka.connect.IDebeziumConfigBuilder;
import org.apache.hudi.debezium.kafka.util.KafkaConnectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Scan connectors and store rdbms connection properties like hostname, username, password and so on.
 */
public class ConnectorScanner {

    private final static Logger logger = LoggerFactory.getLogger(ConnectorScanner.class);

    private final String kafkaConnectUrl;

    private final DebeziumConfigBuilderPrototype debeziumConfigBuilderPrototype;

    private final ConcurrentHashMap<String, DBType> serverNames = new ConcurrentHashMap<>();

    public ConnectorScanner(String kafkaConnectUrl, DebeziumConfigBuilderPrototype debeziumConfigBuilderPrototype) {
        this.kafkaConnectUrl = kafkaConnectUrl;
        this.debeziumConfigBuilderPrototype = debeziumConfigBuilderPrototype;
    }

    public ConnectorScanner addServerName(String serverName, DBType dbType) {
        serverNames.put(serverName, dbType);
        return this;
    }

    public ConnectorScanner removeServerName(String serverName) {
        serverNames.remove(serverName);
        return this;
    }

    public Set<String> getServerNames() {
        return serverNames.keySet();
    }

    public Integer getServerNameSize() {
        return serverNames.size();
    }

    public void scanAllServers() throws IOException, DebeziumConfigBuilderNotFoundException {
        for (String serverName : serverNames.keySet()) {
            scanServerName(serverName);
        }
    }

    public Optional<DebeziumConfig> scanServerName(String serverName) throws IOException, DebeziumConfigBuilderNotFoundException {
        DBType dbType = serverNames.get(serverName);
        IDebeziumConfigBuilder debeziumConfigBuilder = debeziumConfigBuilderPrototype.getDebeziumConfigBuilder(dbType);
        List<String> connectors = KafkaConnectUtils.getConnectors(kafkaConnectUrl);

        DebeziumConfig debeziumConfig = null;
        for (String connector : connectors) {
            Map<String, String> config = KafkaConnectUtils.getConnectorConfig(kafkaConnectUrl, connector);
            debeziumConfig = debeziumConfigBuilder.buildConfig(config);
            if (serverName.equals(debeziumConfig.getServerName())) {

                DebeziumConfig oldConfig = DebeziumConfigCache.CACHE.getDefaultConfig(serverName);
                if (!debeziumConfig.equals(oldConfig)) {
                    logger.info("[config] need to add or update debezium config ({}) to cache \n{}", serverName, debeziumConfig);
                    DebeziumConfigCache.CACHE.addDebeziumConfig(serverName, debeziumConfig);
                }
                break;
            }
        }

        return Optional.ofNullable(debeziumConfig);
    }
}
