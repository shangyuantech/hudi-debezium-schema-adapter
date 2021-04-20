package org.apache.hudi.debezium.kafka.consumer;

import org.apache.hudi.debezium.kafka.config.KafkaConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;


public class ConsumerService<T, R> extends Thread {

    private final static Logger logger = LoggerFactory.getLogger(ConsumerService.class);

    private final String topic;

    private final KafkaConfig kafkaConfig;

    private final KafkaConsumer<T, R> consumer;

    private final IRecordService recordService;

    public ConsumerService(String topic, KafkaConfig kafkaConfig, IRecordService recordService) {
        this.topic = topic;
        this.kafkaConfig = kafkaConfig;
        this.recordService = recordService;
        this.consumer = new KafkaConsumer<>(kafkaConfig.getProps());
    }

    public ConsumerService(String topic, KafkaConfig kafkaConfig, IRecordService recordService, Class<T> t, Class<R> r) {
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

    private boolean needRun = true;

    @Override
    public void run() {
        consumer.subscribe(Collections.singletonList(topic));
        ConsumerRecords<T, R> msgList;
        logger.info("[master] start polling {} topic records ...", topic);

        try {
            while (needRun) {
                msgList = consumer.poll(Duration.ofMillis(1000));
                if (null != msgList && msgList.count() > 0) {
                    for (ConsumerRecord<T, R> record : msgList) {
                        recordService.listen(record);
                    }
                    if ("false".equals(kafkaConfig.getProps().getProperty(
                            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true"))
                    ) {
                        consumer.commitSync();
                    }
                } else {
                    Thread.sleep(1000);
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            consumer.close();
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
