package org.apache.hudi.debezium.kafka.consumer;

import org.apache.commons.lang.StringUtils;
import org.apache.hudi.debezium.config.KafkaConfig;
import org.apache.hudi.debezium.kafka.consumer.record.IRecordService;
import org.apache.hudi.debezium.kafka.consumer.record.SchemaRecord;
import org.apache.hudi.debezium.kafka.topic.TopicHistory;
import org.apache.hudi.debezium.util.JsonUtils;
import org.apache.hudi.debezium.zookeeper.connector.ZookeeperConnector;
import org.apache.hudi.debezium.zookeeper.task.SubTask;
import org.apache.hudi.debezium.zookeeper.task.Task;
import org.apache.hudi.debezium.zookeeper.util.ZooKeeperUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_RECORDS_CONFIG;

public class ConsumerService extends Thread {

    private final static Logger logger = LoggerFactory.getLogger(ConsumerService.class);

    private final String topic;

    private final KafkaConfig kafkaConfig;

    private final KafkaConsumer<?, ?> consumer;

    private final IRecordService recordService;

    private final ZookeeperConnector zkConnector;

    private final TopicHistory topicHistory;

    private final static String HUDI_DBZ_SCHEMA_CHANGE_START_TIME = "HUDI_DBZ_SCHEMA_CHANGE_START_TIME";
    private Long startTime = 0L;

    public ConsumerService(String topic, KafkaConfig kafkaConfig, ZookeeperConnector zkConnector,
                           IRecordService recordService) throws Exception {
        this.topic = topic;
        this.kafkaConfig = kafkaConfig;
        this.zkConnector = zkConnector;
        this.recordService = recordService;

        this.topicHistory = new TopicHistory(zkConnector, topic);

        if (StringUtils.isBlank(kafkaConfig.get(MAX_POLL_RECORDS_CONFIG))) {
            kafkaConfig.addKafkaConfig(MAX_POLL_RECORDS_CONFIG, "100");
        }
        // every time is latest
        kafkaConfig.addKafkaConfig(AUTO_OFFSET_RESET_CONFIG, "latest");
        this.consumer = new KafkaConsumer<>(kafkaConfig.getProps());

        String startTimeStr = System.getenv(HUDI_DBZ_SCHEMA_CHANGE_START_TIME);
        if (StringUtils.isNotBlank(startTimeStr)) {
            startTime = Long.parseLong(startTimeStr);
        }
    }

    public String getTopic() {
        return topic;
    }

    public KafkaConfig getKafkaConfig() {
        return kafkaConfig;
    }

    private volatile boolean needRun = true;

    private ConsumerRecords<?, ?> poll() throws InterruptedException {
        ConsumerRecords<?, ?> msgList = consumer.poll(Duration.ofMillis(TimeUnit.SECONDS.toMillis(1)));
        Thread.sleep(TimeUnit.SECONDS.toMillis(1));
        return msgList;
    }

    @Override
    public void run() {
        consumer.subscribe(Collections.singletonList(topic));
        ConsumerRecords<?, ?> msgList;

        try {
            Map<TopicPartition, Long> offsetMap = new HashMap<>(topicHistory.getOffsets());
            if (offsetMap.size() > 0) {
                logger.info("[master] Found offset history, start from offsets {} ...", offsetMap);
                Set<TopicPartition> assignment = new HashSet<>();
                // poll 1st
                poll();
                while (assignment.size() == 0) {
                    poll();
                    assignment = consumer.assignment();
                }
                logger.info("[master] Found topic {} assignment {}", topic, assignment);

                // deal with assignment
                for (TopicPartition tp : assignment) {
                    Long offset =  offsetMap.get(tp);
                    if (offset != null) {
                        consumer.seek(tp, offset + 1);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        logger.info("[master] Start polling {} topic records ...", topic);
        while (needRun) {
            try {
                msgList = poll();

                if (null != msgList && msgList.count() > 0) {
                    for (ConsumerRecord<?, ?> record : msgList) {
                        // record offset
                        topicHistory.recordOffset(topic, record.partition(), record.offset());

                        if (record.timestamp() < startTime) {
                            if (logger.isTraceEnabled()) {
                                logger.trace("[publish] record {} is before {}, so that skip.",
                                        record.value(), new Timestamp(startTime));
                            }
                            continue;
                        }

                        try {
                            // get schema change record
                            Optional<SchemaRecord> schemaRecord = recordService.listen(record);
                            if (schemaRecord.isPresent()) {
                                // get publish task object
                                Optional<Task<?, ?>> task = recordService.publishTask(schemaRecord.get());

                                // register task to zookeeper
                                if (task.isPresent()) {
                                    registerTask(task.get());
                                }
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }

                    if ("false".equals(kafkaConfig.getProps().getProperty(
                            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true"))
                    ) {
                        consumer.commitSync();
                    }

                    // every loop save kafka offset
                    topicHistory.syncOffsets();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        logger.info("[master] Closing consumer ...");
        consumer.close();
        logger.info("[master] Finished closing consumer !");
    }

    private void registerTask(Task<?, ?> task) throws Exception {
        // register main task, this describe task info
        String topicPath = ZooKeeperUtils.getTopicsPath(zkConnector.getConfig().getService());
        String mainTaskPath = String.format("%s/%s/%s", topicPath, topic, task.getName());

        // create main task path
        registerNode(mainTaskPath, JsonUtils.writeValueAsString(task));

        //  register sub task, this describe actual tasks
        for (SubTask subTask : task.getTasks()) {
            String subTaskPath = String.format("%s/%s", mainTaskPath, subTask.getName());
            registerNode(subTaskPath, JsonUtils.writeValueAsString(subTask));
        }
    }

    private void registerNode(String nodePath, String nodeData) throws Exception {
        if (zkConnector.dataExists(nodePath) == null) {
            try {
                zkConnector.createNode(nodePath, nodeData);
            } catch (KeeperException e) {
                e.printStackTrace();
                if (e.code() == KeeperException.Code.NODEEXISTS || e.code() == KeeperException.Code.BADVERSION) {
                    logger.warn("[publish] node may be exists, " + nodePath, e);
                } else {
                    throw e;
                }
            }
        }
    }

    public void stopConsumer() {
        try {
            needRun = false;
            //consumer.close();
            topicHistory.removeTopic();
        } catch (Exception e) {
            logger.error("error when closing consumer " + topic, e);
        }
    }
}
