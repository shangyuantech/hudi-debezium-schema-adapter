package org.apache.hudi.debezium.kafka.consumer;

import org.apache.hudi.debezium.config.KafkaConfig;
import org.apache.hudi.debezium.kafka.consumer.record.IRecordService;
import org.apache.hudi.debezium.kafka.consumer.record.SchemaRecord;
import org.apache.hudi.debezium.util.JsonUtils;
import org.apache.hudi.debezium.zookeeper.connector.ZookeeperConnector;
import org.apache.hudi.debezium.zookeeper.task.SubTask;
import org.apache.hudi.debezium.zookeeper.task.Task;
import org.apache.hudi.debezium.zookeeper.util.ZooKeeperUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.TimeUnit;


public class ConsumerService extends Thread {

    private final static Logger logger = LoggerFactory.getLogger(ConsumerService.class);

    private final String topic;

    private final KafkaConfig kafkaConfig;

    private final KafkaConsumer<?, ?> consumer;

    private final IRecordService recordService;

    private final ZookeeperConnector zkConnector;

    public ConsumerService(String topic, KafkaConfig kafkaConfig, ZookeeperConnector zkConnector, IRecordService recordService) {
        this.topic = topic;
        this.kafkaConfig = kafkaConfig;
        this.zkConnector = zkConnector;
        this.recordService = recordService;
        this.consumer = new KafkaConsumer<>(kafkaConfig.getProps());
    }

    public String getTopic() {
        return topic;
    }

    public KafkaConfig getKafkaConfig() {
        return kafkaConfig;
    }

    private volatile boolean needRun = true;

    @Override
    public void run() {
        consumer.subscribe(Collections.singletonList(topic));
        ConsumerRecords<?, ?> msgList;
        logger.info("[master] start polling {} topic records ...", topic);

        while (needRun) {
            try {
                msgList = consumer.poll(Duration.ofMillis(TimeUnit.SECONDS.toMillis(1)));
                if (null != msgList && msgList.count() > 0) {

                    for (ConsumerRecord<?, ?> record : msgList) {
                        try {
                            // get schema change record
                            SchemaRecord schemaRecord = recordService.listen(record);

                            // get publish task object
                            Optional<Task<?>> task = recordService.publishTask(schemaRecord);

                            // register task to zookeeper
                            if (task.isPresent()) {
                                registerTask(task.get());
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
                } else {
                    Thread.sleep(TimeUnit.SECONDS.toMillis(1));
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        consumer.close();
    }

    private void registerTask(Task<?> task) throws Exception {
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
            consumer.close();
        } catch (Exception e) {
            logger.error("error when closing consumer " + topic, e);
        }
    }
}
