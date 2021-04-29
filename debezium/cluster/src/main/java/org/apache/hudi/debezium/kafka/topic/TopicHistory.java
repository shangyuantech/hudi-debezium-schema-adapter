package org.apache.hudi.debezium.kafka.topic;

import org.apache.commons.lang.StringUtils;
import org.apache.hudi.debezium.zookeeper.connector.ZookeeperConnector;
import org.apache.hudi.debezium.zookeeper.util.ZooKeeperUtils;
import org.apache.kafka.common.TopicPartition;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TopicHistory {

    private final static Logger logger = LoggerFactory.getLogger(TopicHistory.class);

    private final String topic;

    private final String topicPath;

    private final ZookeeperConnector zkConnector;

    private final ConcurrentHashMap<TopicPartition, Long> offsetsMap;

    public TopicHistory(ZookeeperConnector zkConnector, String topic) throws Exception {
        this.zkConnector = zkConnector;
        this.topic = topic;

        // create kafka offset path
        this.topicPath = getTopicPath();
        if (zkConnector.dataExists(topicPath) == null) {
            try {
                zkConnector.createNode(topicPath);
            } catch (KeeperException.NodeExistsException e) {
                logger.warn("[topic] Kafka node has been created", e);
            }
        }

        offsetsMap = new ConcurrentHashMap<>();
    }

    public Map<TopicPartition, Long> getOffsets() throws Exception {
        Map<TopicPartition, Long> offsetMap = new HashMap<>();
        if (zkConnector.dataExists(topicPath) != null) {
            String checkpointStr = zkConnector.getData(topicPath);
            if (StringUtils.isNotBlank(checkpointStr)) {
                String[] splits = checkpointStr.split(",");
                for (int i = 0; i < splits.length; i++) {
                    String[] subSplits = splits[i].split(":");
                    offsetMap.put(new TopicPartition(topic, Integer.parseInt(subSplits[0])), Long.parseLong(subSplits[1]));
                }
            }
        }

        if (logger.isTraceEnabled()) {
            logger.trace("[topic] Get topic {} offset history {}", topic, offsetMap);
        }
        return offsetMap;
    }

    public void recordOffset(String topic, Integer partition, Long offset) {
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        if (logger.isTraceEnabled()) {
            logger.trace("[topic] Record offset {} with offset {}", topicPartition, offset);
        }

        if (offsetsMap.containsKey(topicPartition)) {
            Long currentOffset = offsetsMap.get(topicPartition);
            if (currentOffset < offset) {
                offsetsMap.put(topicPartition, offset);
            }
        } else {
            offsetsMap.put(topicPartition, offset);
        }
    }

    public void syncOffsets() throws Exception{
        if (logger.isDebugEnabled()) {
            logger.debug("[topic] Start to sync offset {} to topic {} history ...", offsetsMap, topic);
        }

        if (offsetsMap.isEmpty()) {
            logger.info("[topic] No offset in topic {} to save, skip!", topic);
        } else {
            final StringBuilder checkpointStr = new StringBuilder();
            for (Map.Entry<TopicPartition, Long> offset : offsetsMap.entrySet()) {
                checkpointStr.append(",").append(offset.getKey().partition()).append(":").append(offset.getValue());
            }

            zkConnector.setData(topicPath, checkpointStr.substring(1));
        }
    }

    public void removeTopic() throws Exception {
        logger.info("[topic] Delete topic {} offset history", topic);
        if (zkConnector.dataExists(topicPath) == null) {
            zkConnector.delete(topicPath);
        }
    }

    public String getTopicPath() {
        return String.format("%s/%s", ZooKeeperUtils.getKafkaPath(zkConnector.getConfig().getService()), topic);
    }
}
