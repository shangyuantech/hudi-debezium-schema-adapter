package org.apache.hudi.debezium.mysql.impl.master;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.debezium.common.TopicConfig;
import org.apache.hudi.debezium.kafka.config.KafkaConfig;
import org.apache.hudi.debezium.kafka.consumer.record.IRecordService;
import org.apache.hudi.debezium.kafka.consumer.record.SchemaRecord;
import org.apache.hudi.debezium.kafka.util.AvroUtils;
import org.apache.hudi.debezium.mysql.data.MySQLSchemaChange;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySQLRecordService implements IRecordService {

    private final TopicConfig topicConfig;

    private final KafkaConfig kafkaConfig;

    private final Class<?> valueDesClass;

    private final ObjectMapper stringMapper = new ObjectMapper();

    public MySQLRecordService(TopicConfig topicConfig, KafkaConfig kafkaConfig, Class<?> valueDesClass) {
        this.topicConfig = topicConfig;
        this.kafkaConfig = kafkaConfig;
        this.valueDesClass = valueDesClass;
    }

    private final static Logger logger = LoggerFactory.getLogger(MySQLRecordService.class);

    @Override
    public MySQLSchemaChange listen(ConsumerRecord<?, ?> record) throws Exception {
        if (logger.isDebugEnabled()) {
            logger.debug("[master] receive mysql debezium schema change consumer record \n{}", record);
        }

        // cast record to schema change bean
        MySQLSchemaChange schemaChange;
        if (valueDesClass.equals(KafkaAvroDeserializer.class)) {
            schemaChange = AvroUtils.transformAvroToObject((GenericRecord) record.value(), MySQLSchemaChange.class);
        } else {// default is org.apache.kafka.common.serialization.StringDeserializer
            schemaChange = stringMapper.readValue(String.valueOf(record.value()), MySQLSchemaChange.class);
        }

        return schemaChange;
    }

    @Override
    public void publishTask(SchemaRecord record) throws Exception {
        String kafkaConnectUrl = topicConfig.getKafkaConnectUrl();
    }
}
