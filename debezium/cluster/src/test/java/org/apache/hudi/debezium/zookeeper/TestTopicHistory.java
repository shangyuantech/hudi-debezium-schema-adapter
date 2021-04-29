package org.apache.hudi.debezium.zookeeper;

import net.mguenther.kafka.junit.EmbeddedKafkaCluster;
import net.mguenther.kafka.junit.EmbeddedKafkaClusterConfig;
import net.mguenther.kafka.junit.KeyValue;
import org.apache.curator.test.TestingServer;
import org.apache.hudi.debezium.config.ZookeeperConfig;
import org.apache.hudi.debezium.kafka.topic.TopicHistory;
import org.apache.hudi.debezium.util.JsonUtils;
import org.apache.hudi.debezium.zookeeper.connector.ZookeeperConnector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static net.mguenther.kafka.junit.EmbeddedKafkaCluster.provisionWith;
import static net.mguenther.kafka.junit.ObserveKeyValues.on;
import static net.mguenther.kafka.junit.SendKeyValues.to;
import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.CommonClientConfigs.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

public class TestTopicHistory {

    private TestingServer testingServer;

    private EmbeddedKafkaCluster kafka;

    @Before
    public void before() throws Exception {
        // start zookeeper
        File tmp = new File("/tmp/zookeeper/test_topic_history");
        if (tmp.exists()) {
            deleteDir(tmp);
        }
        tmp.mkdirs();

        testingServer = new TestingServer(2181, tmp);
        testingServer.start();

        // start kafka
        kafka = provisionWith(EmbeddedKafkaClusterConfig.useDefaults());
        kafka.start();
    }

    private void deleteDir(File dir) {
        if (dir.isDirectory()) {
            String[] children = dir.list();
            for (int i = 0; i < Objects.requireNonNull(children).length; i++) {
                deleteDir(new File(dir, children[i]));
            }
        }
        dir.delete();
    }

    private final static String testOffset = "0:100,1:200,2:300";

    @Test
    public void testInstance() throws Exception {
        ZookeeperConnector zkConnector = new ZookeeperConnector(new ZookeeperConfig());
        TopicHistory topicHistory = new TopicHistory(zkConnector, "test_topic1");

        zkConnector.setData(topicHistory.getTopicPath(), testOffset);

        Map<TopicPartition, Long> offsetMap = topicHistory.getOffsets();
        Assert.assertEquals(offsetMap.size(), 3);
        Assert.assertEquals(offsetMap.get(new TopicPartition("test_topic1", 0)).longValue(), 100);
    }

    @Test
    public void testKafkaTopicOffset() throws Exception {
        ZookeeperConnector zkConnector = new ZookeeperConnector(new ZookeeperConfig());
        String topic = "test_topic2";
        TopicHistory topicHistory = new TopicHistory(zkConnector, topic);

        kafka.send(to(topic, Arrays.asList(
                new KeyValue<>("test_topic2_key1", "record1"),
                new KeyValue<>("test_topic2_key2", "record2")))
                .build());

        kafka.observe(on(topic, 2).build());

        Properties kafkaProps = new Properties();
        kafkaProps.put(BOOTSTRAP_SERVERS_CONFIG, kafka.getBrokerList());
        kafkaProps.put(KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.put(VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.put(GROUP_ID_CONFIG, "test_group");
        kafkaProps.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
        kafkaProps.put(ENABLE_AUTO_COMMIT_CONFIG, true);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(kafkaProps);
        consumer.subscribe(Collections.singletonList(topic));

        ConsumerRecords<String, String> msgList = poll(consumer);
        long offset = 0L;
        for (ConsumerRecord<?, ?> record : msgList) {
            offset = record.offset();
        }
        topicHistory.recordOffset(topic, 0, offset);

        consumer.seek(new TopicPartition(topic, 0), offset + 1);

        poll(consumer);
        poll(consumer);

        kafka.send(to(topic, Arrays.asList(
                new KeyValue<>("test_topic2_key3", "record3"),
                new KeyValue<>("test_topic2_key4", "record4")))
                .build());
        kafka.observe(on(topic, 4).build());

        msgList = poll(consumer);
        msgList.records(topic).forEach(System.out::println);
        Assert.assertEquals(msgList.count(), 2);
    }

    private ConsumerRecords<String, String> poll(KafkaConsumer<String, String> consumer) {
        return consumer.poll(Duration.ofMillis(TimeUnit.SECONDS.toMillis(1)));
    }

}
