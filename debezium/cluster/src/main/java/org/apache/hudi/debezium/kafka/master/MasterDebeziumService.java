package org.apache.hudi.debezium.kafka.master;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hudi.debezium.common.TopicConfig;
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

    private final DebeziumTopicTaskPrototype dbTopicTaskProt;

    private final ZookeeperConnector zkConnector;

    private final String topicPath;

    public MasterDebeziumService(ZookeeperConnector zkConnector, DebeziumTopicTaskPrototype dbTopicTaskProt) {
        this.zkConnector = zkConnector;
        this.dbTopicTaskProt = dbTopicTaskProt;
        this.topicPath = ZooKeeperUtils.getTopicsPath(zkConnector.getConfig().getService());
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
        ObjectMapper objectMapper = new ObjectMapper();
        while (leader) {
            List<String> subscribedTopics = getSubscribedTopics();
            // delete removed topic
            for (Map.Entry<String, IDebeziumTopicTask> topicTask : topicTasks.entrySet()) {
                if (!subscribedTopics.contains(topicTask.getKey())) {
                    logger.info("[master] stop topic task {} by topic removed action", topicTask.getKey());
                    topicTask.getValue().stop();
                }
            }

            // add new topic
            for (String topic : subscribedTopics) {
                if (!topicTasks.containsKey(topic)) {
                    String topicConfigStr = zkConnector.getData(String.format("%s/%s", topicPath, topic));
                    TopicConfig topicConfig = objectMapper.readValue(topicConfigStr, TopicConfig.class);

                    logger.info("[master] start a new topic({}) task ...", topic);
                    IDebeziumTopicTask debeziumTopicTask = dbTopicTaskProt.getTopicTask(topicConfig.getDbType());
                    debeziumTopicTask.start(topic, topicConfig);
                    topicTasks.put(topic, debeziumTopicTask);
                }
            }

            Thread.sleep(TimeUnit.SECONDS.toMillis(zkConnector.getConfig().getMasterLoopInterval()));
        }
    }

    @Override
    public void notLeader() throws Exception {
        leader = false;
        for (Map.Entry<String, IDebeziumTopicTask> topicTask : topicTasks.entrySet()) {
            logger.info("[master] stop topic task {} by not leader action", topicTask.getKey());
            topicTask.getValue().stop();
        }
    }
}
