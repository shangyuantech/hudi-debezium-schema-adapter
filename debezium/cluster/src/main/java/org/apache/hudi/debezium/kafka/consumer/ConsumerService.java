package org.apache.hudi.debezium.kafka.consumer;

import org.apache.hudi.debezium.common.TopicConfig;
import org.apache.hudi.debezium.kafka.config.KafkaConfig;
import org.apache.hudi.debezium.kafka.consumer.record.IRecordService;
import org.apache.hudi.debezium.kafka.consumer.record.SchemaRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.TimeUnit;


public class ConsumerService extends Thread {

    private final static Logger logger = LoggerFactory.getLogger(ConsumerService.class);

    private final String topic;

    private final KafkaConfig kafkaConfig;

    private final KafkaConsumer<?, ?> consumer;

    private final IRecordService recordService;

    public ConsumerService(String topic, KafkaConfig kafkaConfig, IRecordService recordService) {
        this.topic = topic;
        this.kafkaConfig = kafkaConfig;
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
                            SchemaRecord schemaRecord = recordService.listen(record);
                            recordService.publishTask(schemaRecord);
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

    public void stopConsumer() {
        try {
            needRun = false;
            consumer.close();
        } catch (Exception e) {
            logger.error("error when closing consumer " + topic, e);
        }
    }
}
