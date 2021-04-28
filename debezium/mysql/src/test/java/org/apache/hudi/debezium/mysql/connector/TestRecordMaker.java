package org.apache.hudi.debezium.mysql.connector;

import ch.vorburger.mariadb4j.DBConfigurationBuilder;
import ch.vorburger.mariadb4j.junit.MariaDB4jRule;
import com.fasterxml.jackson.core.JsonProcessingException;
import io.confluent.connect.avro.AvroConverter;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.debezium.config.Configuration;
import io.debezium.connector.mysql.*;
import io.debezium.connector.mysql.MySQLRecordProcessor;
import io.debezium.relational.history.FileDatabaseHistory;
import net.mguenther.kafka.junit.EmbeddedKafkaCluster;
import net.mguenther.kafka.junit.EmbeddedKafkaClusterConfig;
import org.apache.hudi.debezium.mysql.TestSchemaChange;
import org.apache.hudi.debezium.mysql.data.MySQLSchemaChange;
import org.apache.hudi.debezium.util.JsonUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.Converter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

import static net.mguenther.kafka.junit.EmbeddedKafkaCluster.provisionWith;
import static net.mguenther.kafka.junit.ObserveKeyValues.on;

public class TestRecordMaker {

    DBConfigurationBuilder configBuilder = DBConfigurationBuilder.newBuilder()
            .setPort(3307)
            .setDataDir("/tmp/mysql/test_database");

    @Rule
    public MariaDB4jRule dbRule = new MariaDB4jRule(
            configBuilder.build(),
            "test_database",
            "sql/test_table.sql"
    );

    private EmbeddedKafkaCluster kafka;

    public TestRecordMaker() throws JsonProcessingException {
    }

    @Before
    public void before() throws Exception {
        kafka = provisionWith(EmbeddedKafkaClusterConfig.useDefaults());
        kafka.start();
    }

    private MockSchemaRegistryClient ceSchemaRegistry = new MockSchemaRegistryClient();

    /**
     * @return Configuration builder initialized with JDBC connection parameters.
     */
    public Configuration.Builder defaultJdbcConfigBuilder() {
        return Configuration.create()
                .with(MySqlConnectorConfig.HOSTNAME, System.getProperty("database.hostname", "localhost"))
                .with(MySqlConnectorConfig.PORT, System.getProperty("database.port", "3307"))
                .with(MySqlConnectorConfig.USER, "root")
                .with(MySqlConnectorConfig.PASSWORD, "")
                .with(MySqlConnectorConfig.SERVER_NAME, "cluster_mysql_test")
                .with(MySqlConnectorConfig.DATABASE_WHITELIST, "test_database")
                .with(MySqlConnectorConfig.TABLE_INCLUDE_LIST, "test_database.test_table")
                .with(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .with(MySqlConnectorConfig.DATABASE_HISTORY, FileDatabaseHistory.class)
                .with(FileDatabaseHistory.FILE_PATH, "/tmp/debezium/file-db-history-enum-column.txt");
    }

    private final MySQLSchemaChange schemaChange = JsonUtils.readValue(TestSchemaChange.data, MySQLSchemaChange.class);

    @Test
    public void testAddColumn() throws InterruptedException, RestClientException, IOException {
        String sql = "ALTER TABLE test_database.test_table ADD deptno int";
        schemaChange.setDdl(sql);
        testRecordMakerReadRow();
    }

    @Test
    public void testReplaceColumn() throws InterruptedException, RestClientException, IOException {
        String sql = "ALTER TABLE test_table CHANGE deptno_before deptno int";
        schemaChange.setDdl(sql);
        testRecordMakerReadRow();
    }

    public void testRecordMakerReadRow()
            throws InterruptedException, IOException, RestClientException {

        MySQLRecordProcessor processor = new MySQLRecordProcessor("test_database", "test_table",
                defaultJdbcConfigBuilder().build(), "", schemaChange);
        Assert.assertNotNull(processor);
        processor.startTask();

        Converter keyConverter = getConverter(true);
        Converter valueConverter = getConverter(false);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBrokerList());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.ByteArraySerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.ByteArraySerializer.class);
        props.put("schema.registry.url", "http://fake-url");

        //KafkaAvroSerializer avroSerializer = new KafkaAvroSerializer(ceSchemaRegistry);
        KafkaProducer<Object, Object> producer = new KafkaProducer<>(props);

        processor.pollData(data -> {
            for (SourceRecord record : data) {
                System.out.println(record);
                byte[] key = keyConverter.fromConnectData(record.topic(), record.keySchema(), record.key());
                byte[] value = valueConverter.fromConnectData(record.topic(), record.valueSchema(), record.value());

                //SchemaAndValue avroSchemaAndValue = converter.toConnectData(record.topic(), value);
                //System.out.println(avroSchemaAndValue.toString());

                ProducerRecord<Object, Object> sendData = new ProducerRecord<>(record.topic(), key, value);
                try {
                    producer.send(sendData);
                } catch(SerializationException e) {
                    e.printStackTrace();
                }
            }
            producer.flush();
        });

        producer.close();
        kafka.observe(on("cluster_mysql_test.test_database.test_table", 4).build());

        System.out.println(ceSchemaRegistry.getAllSubjects());

        // get data
        //props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        //props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        //props.put("schema.registry.url", "http://fake-url");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(CommonClientConfigs.GROUP_ID_CONFIG, "test_group");

        KafkaAvroDeserializer kafkaDeserializer = new KafkaAvroDeserializer(ceSchemaRegistry);
        final Consumer consumer = new KafkaConsumer<>(props, kafkaDeserializer, kafkaDeserializer);
        consumer.subscribe(Arrays.asList("cluster_mysql_test.test_database.test_table"));
        try {
            ConsumerRecords<?, ?> records = consumer.poll(100);
            for (ConsumerRecord<?, ?> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s \n", record.offset(), record.key(), record.value());
            }
        } catch(Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }

    public Converter getConverter(boolean isKey) {
        Map<String, Object> config = new HashMap<>();
        config.put("schema.registry.url", "http://fake-url");

        Converter avroConverter = new AvroConverter(ceSchemaRegistry);
        avroConverter.configure(config, isKey);

        return avroConverter;
    }
}
