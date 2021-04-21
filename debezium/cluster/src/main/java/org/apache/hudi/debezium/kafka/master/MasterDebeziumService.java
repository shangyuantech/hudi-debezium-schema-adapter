package org.apache.hudi.debezium.kafka.master;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hudi.debezium.common.TopicConfig;
import org.apache.hudi.debezium.kafka.connect.DebeziumConfigBuilderPrototype;
import org.apache.hudi.debezium.kafka.connect.scanner.ConnectorScannerTask;
import org.apache.hudi.debezium.kafka.master.task.DebeziumTopicTaskPrototype;
import org.apache.hudi.debezium.kafka.master.task.IDebeziumTopicTask;
import org.apache.hudi.debezium.zookeeper.connector.ZookeeperConnector;
import org.apache.hudi.debezium.zookeeper.master.IMasterZkService;
import org.apache.hudi.debezium.zookeeper.util.ZooKeeperUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class MasterDebeziumService implements IMasterZkService {

    private final static Logger logger = LoggerFactory.getLogger(MasterDebeziumService.class);

    private final ObjectMapper objectMapper = new ObjectMapper();

    private final DebeziumTopicTaskPrototype dbTopicTaskPrototype;

    private final DebeziumConfigBuilderPrototype configBuilderPrototype;

    private final ZookeeperConnector zkConnector;

    private final String topicPath;

    private boolean scannerTaskEnabled = false;

    private ConnectorScannerTask scanner = null;

    public MasterDebeziumService(ZookeeperConnector zkConnector,
                                 DebeziumTopicTaskPrototype dbTopicTaskPrototype,
                                 DebeziumConfigBuilderPrototype configBuilderPrototype) {
        this.zkConnector = zkConnector;
        this.dbTopicTaskPrototype = dbTopicTaskPrototype;
        this.configBuilderPrototype = configBuilderPrototype;
        this.topicPath = ZooKeeperUtils.getTopicsPath(zkConnector.getConfig().getService());
    }

    public MasterDebeziumService(ZookeeperConnector zkConnector,
                                 DebeziumTopicTaskPrototype dbTopicTaskPrototype,
                                 DebeziumConfigBuilderPrototype configBuilderPrototype,
                                 boolean scannerTaskEnabled) {
        this(zkConnector, dbTopicTaskPrototype, configBuilderPrototype);
        this.scannerTaskEnabled = scannerTaskEnabled;
    }

    private List<String> getSubscribedTopics() throws Exception {
        return zkConnector.dataExists(topicPath) == null ? Collections.emptyList() : zkConnector.getChildes(topicPath);
    }

    public String getTopicPath() {
        return topicPath;
    }

    private final HashMap<String, IDebeziumTopicTask> topicTasks = new HashMap<>();

    private boolean leader = true;

    @Override
    public void isLeader() throws Exception {

        if (scannerTaskEnabled) {
            // start a connector scanner task to sync debezium config
            scanner = new ConnectorScannerTask(configBuilderPrototype);
            for (String topic : getSubscribedTopics()) {
                scanner.addConnector(topic, getTopicConfig(topic));
            }

            scanner.start();
            // waite 3s to sync debezium config
            Thread.sleep(TimeUnit.SECONDS.toMillis(3));
        }

        while (leader) {
            List<String> subscribedTopics = getSubscribedTopics();
            // delete removed topic
            for (Map.Entry<String, IDebeziumTopicTask> topicTask : topicTasks.entrySet()) {
                if (!subscribedTopics.contains(topicTask.getKey())) {

                    logger.info("[master] stop topic task {} by topic removed action", topicTask.getKey());
                    topicTask.getValue().stop();

                    if (scannerTaskEnabled) {
                        scanner.removeConnector(topicTask.getKey());
                    }
                }
            }

            // add new topic
            for (String topic : subscribedTopics) {
                if (!topicTasks.containsKey(topic)) {
                    TopicConfig topicConfig = getTopicConfig(topic);

                    if (scannerTaskEnabled) {
                        scanner.addConnector(topic, topicConfig);
                    }

                    logger.info("[master] start a new topic({}) task ...", topic);
                    IDebeziumTopicTask debeziumTopicTask = dbTopicTaskPrototype.getTopicTask(topicConfig.getDbType());
                    debeziumTopicTask.start(topic, topicConfig);
                    topicTasks.put(topic, debeziumTopicTask);
                }
            }

            Thread.sleep(TimeUnit.SECONDS.toMillis(zkConnector.getConfig().getMasterLoopInterval()));
        }
    }

    private TopicConfig getTopicConfig(String topic) throws Exception {
        String topicConfigStr = zkConnector.getData(String.format("%s/%s", topicPath, topic));
        return objectMapper.readValue(topicConfigStr, TopicConfig.class);
    }

    @Override
    public void notLeader() throws Exception {
        leader = false;

        // stop topic task
        for (Map.Entry<String, IDebeziumTopicTask> topicTask : topicTasks.entrySet()) {
            logger.info("[master] stop topic task {} by not leader action", topicTask.getKey());
            topicTask.getValue().stop();
        }

        //  stop scanner task
        if (scannerTaskEnabled) {
            scanner.stopTask();
            scanner = null;
        }
    }
}
