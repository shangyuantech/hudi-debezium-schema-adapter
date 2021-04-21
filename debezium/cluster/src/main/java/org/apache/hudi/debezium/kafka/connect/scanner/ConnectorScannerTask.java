package org.apache.hudi.debezium.kafka.connect.scanner;

import org.apache.hudi.debezium.common.TopicConfig;
import org.apache.hudi.debezium.common.exception.DebeziumConfigBuilderNotFoundException;
import org.apache.hudi.debezium.kafka.config.DebeziumConfigCache;
import org.apache.hudi.debezium.kafka.connect.DebeziumConfigBuilderPrototype;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class ConnectorScannerTask extends Thread {

    private final static Logger logger = LoggerFactory.getLogger(ConnectorScannerTask.class);

    public volatile boolean exit = false;

    private final DebeziumConfigBuilderPrototype debeziumConfigBuilderPrototype;

    public ConnectorScannerTask(DebeziumConfigBuilderPrototype debeziumConfigBuilderPrototype) {
        this.debeziumConfigBuilderPrototype = debeziumConfigBuilderPrototype;
    }

    private final HashMap<String, ConnectorScanner> connectorScanners = new HashMap<>();

    public void addConnector(String serverName, TopicConfig topicConfig) {
        String kafkaConnectUrl = topicConfig.getKafkaConnectUrl();

        if (!connectorScanners.containsKey(kafkaConnectUrl)) {
            ConnectorScanner scanner = new ConnectorScanner(kafkaConnectUrl, debeziumConfigBuilderPrototype);
            scanner.addServerName(serverName, topicConfig.getDbType());
            connectorScanners.put(kafkaConnectUrl, scanner);
        } else {
            ConnectorScanner scanner = connectorScanners.get(kafkaConnectUrl);
            Set<String> existsServerNames = scanner.getServerNames();
            if (!existsServerNames.contains(serverName)) {
                scanner.addServerName(serverName, topicConfig.getDbType());
            }
        }
    }

    public void removeConnector(String serverName) {
        for (ConnectorScanner scanner : connectorScanners.values()) {
            Set<String> serverNames = scanner.getServerNames();
            if (serverNames.contains(serverName)) {
                logger.info("[config] remove debezium config by serverName = {}", serverName);
                scanner.removeServerName(serverName);
                // todo need to remove cache, but not sure it is safe
                DebeziumConfigCache.CACHE.removeDefaultConfig(serverName);
            }
        }
    }

    public void run() {
        while (!exit){
            if (connectorScanners.size() > 0) {
                if (logger.isDebugEnabled()) {
                    logger.debug("[config] start to scan kafka connectors to get debezium config and check if updated ...");
                }
                for (Map.Entry<String, ConnectorScanner> entry : connectorScanners.entrySet()) {
                    ConnectorScanner scanner = entry.getValue();
                    if (scanner.getServerNameSize() == 0) {
                        logger.info("[config] remove scanner task by kafka connect url = {}", entry.getKey());
                        connectorScanners.remove(entry.getKey());
                    } else {
                        try {
                            scanner.scanAllServers();
                        } catch (IOException | DebeziumConfigBuilderNotFoundException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }

            try {
                Thread.sleep(TimeUnit.SECONDS.toMillis(10));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        logger.info("[config] finished scanning kafka connectors ...");
    }

    public void stopTask() {
        exit = true;
    }
}
