package org.apache.hudi.debezium.mysql.impl.master;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.debezium.common.DBType;
import org.apache.hudi.debezium.common.TopicConfig;
import org.apache.hudi.debezium.common.exception.DebeziumConfigNotFoundException;
import org.apache.hudi.debezium.kafka.config.DebeziumConfig;
import org.apache.hudi.debezium.kafka.config.DebeziumConfigCache;
import org.apache.hudi.debezium.kafka.config.KafkaConfig;
import org.apache.hudi.debezium.kafka.connect.DebeziumConfigBuilderPrototype;
import org.apache.hudi.debezium.kafka.connect.scanner.ConnectorScanner;
import org.apache.hudi.debezium.kafka.consumer.record.IRecordService;
import org.apache.hudi.debezium.kafka.consumer.record.SchemaRecord;
import org.apache.hudi.debezium.kafka.util.AvroUtils;
import org.apache.hudi.debezium.mysql.data.MySQLDebeziumConfig;
import org.apache.hudi.debezium.mysql.data.MySQLSchemaChange;
import org.apache.hudi.debezium.mysql.impl.connect.MySQLDebeziumConfigBuilder;
import org.apache.hudi.schema.ddl.DDLStat;
import org.apache.hudi.schema.parser.DefaultSchemaParser;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

import static org.apache.hudi.schema.common.DDLType.ALTER_ADD_COL;

public class MySQLRecordService implements IRecordService {

    private final static DefaultSchemaParser schemaParser = new DefaultSchemaParser();

    private final String topic;

    private final TopicConfig topicConfig;

    private final KafkaConfig kafkaConfig;

    private final Class<?> valueDesClass;

    private final ObjectMapper stringMapper = new ObjectMapper();

    public MySQLRecordService(String topic, TopicConfig topicConfig, KafkaConfig kafkaConfig, Class<?> valueDesClass) {
        this.topic = topic;
        this.topicConfig = topicConfig;
        this.kafkaConfig = kafkaConfig;
        this.valueDesClass = valueDesClass;
    }

    private final static Logger logger = LoggerFactory.getLogger(MySQLRecordService.class);

    @Override
    public MySQLSchemaChange listen(ConsumerRecord<?, ?> record) throws Exception {
        if (logger.isDebugEnabled()) {
            logger.debug("[master] receive mysql debezium schema change consumer record \nkey = {}, value = {}",
                    record.key(), record.value());
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
        MySQLDebeziumConfig mysqlConfig = (MySQLDebeziumConfig) DebeziumConfigCache.CACHE.getDefaultConfig(topic);
        String ddl = ((MySQLSchemaChange) record).getDdl();

        // check if this sql need to publish task
        DDLStat ddlStat = schemaParser.getSqlStat(ddl);
        if (ddlStat.getDdlType() != ALTER_ADD_COL) {
            if (logger.isDebugEnabled()) {
                logger.debug("[master] no need to publish ddl `{}`", ddl);
            }
        }

        // if null, need to start a task to sync config
        if (mysqlConfig == null) {
            if (logger.isDebugEnabled()) {
                logger.debug("[master] can not find `{}` debezium config, needs to run scanner task independently ...", topic);
            }

            DebeziumConfigBuilderPrototype configBuilderPrototype = new DebeziumConfigBuilderPrototype()
                    .addDebeziumConfigBuilder(DBType.MySQL, new MySQLDebeziumConfigBuilder());
            ConnectorScanner scanner = new ConnectorScanner(kafkaConnectUrl, configBuilderPrototype)
                    .addServerName(topic, DBType.MySQL);
            Optional<DebeziumConfig> debeziumConfig = scanner.scanServerName(topic);

            if (!debeziumConfig.isPresent()) {
                throw new DebeziumConfigNotFoundException("can not get debezium config! topic = " + topic);
            }
            mysqlConfig = (MySQLDebeziumConfig) debeziumConfig.get();
        }

        // get jdbc link and get table schema
    }
}
