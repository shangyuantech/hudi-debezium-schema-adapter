package org.apache.hudi.debezium.mysql.impl.slave;

import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySQLRecordProcessor;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.relational.history.FileDatabaseHistory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.hudi.debezium.common.DBType;
import org.apache.hudi.debezium.common.TopicConfig;
import org.apache.hudi.debezium.config.KafkaConfig;
import org.apache.hudi.debezium.kafka.producer.ChangeDataProducer;
import org.apache.hudi.debezium.kafka.producer.record.RecordConverterFactory;
import org.apache.hudi.debezium.mysql.data.MySQLDebeziumConfig;
import org.apache.hudi.debezium.mysql.data.MySQLTask;
import org.apache.hudi.debezium.mysql.jdbc.JDBCUtils;
import org.apache.hudi.debezium.util.JsonUtils;
import org.apache.hudi.debezium.zookeeper.slave.task.ISlaveTask;
import org.apache.hudi.debezium.zookeeper.task.SubTask;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.Converter;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MySQLSlaveTask implements ISlaveTask {

    private final static Logger logger = LoggerFactory.getLogger(MySQLSlaveTask.class);

    @Override
    public DBType getDbType() {
        return DBType.MySQL;
    }

    @Override
    public void eventTrigger(CuratorFramework client, TreeCacheEvent event, String topicName, TopicConfig topicConfig) throws Exception {
        ChildData data = event.getData();
        String subTaskConfigStr = new String(event.getData().getData());
        SubTask subTask = JsonUtils.readValue(subTaskConfigStr, SubTask.class);
        String taskName = subTask.getName();
        if (logger.isDebugEnabled()) {
            logger.debug("[slave] [task: {}] start to run task with data = {}", taskName, subTaskConfigStr);
        }

        String mainTaskPath = Paths.get(data.getPath()).getParent().toString();
        MySQLTask mysqlTask = null;
        KafkaConfig kafkaConfig = new KafkaConfig(topicConfig.getKafkaConfigProperties());

        try {
            // get debezium config
            String mainTaskConfig = new String(client.getData().forPath(mainTaskPath));
            if (logger.isDebugEnabled()) {
                logger.debug("[slave] [task: {}] get main mysql task config = \n{}", taskName, mainTaskConfig);
            }
            mysqlTask = JsonUtils.readValue(mainTaskConfig, MySQLTask.class);
        } catch (KeeperException.NodeExistsException e) {
            logger.warn(String.format(
                    "[slave] [task: %s] Can not find main task config, maybe task has been cancelled!", taskName),
                    e);
            return;
        }

        MySQLDebeziumConfig mysqlConfig = mysqlTask.getDebeziumConfig();
        String database = subTask.getDatabase();
        String table = subTask.getTable();
        Configuration conf = defaultJdbcConfigBuilder(mysqlConfig, database, table).build();

        MySQLRecordProcessor processor = new MySQLRecordProcessor(database, table,
                conf, mysqlTask.getSchemaRecord());
        processor.startTask();

        // poll data and send to kafka
        submitRecord(kafkaConfig, processor);

        // stop
        processor.stopTask();
    }

    private Configuration.Builder defaultJdbcConfigBuilder(MySQLDebeziumConfig mysqlConfig,
                                                          String database, String table) {
        return Configuration.create()
                .with(MySqlConnectorConfig.HOSTNAME, mysqlConfig.getHostname())
                .with(MySqlConnectorConfig.PORT, mysqlConfig.getPort())
                .with(MySqlConnectorConfig.USER, mysqlConfig.getUser())
                .with(MySqlConnectorConfig.PASSWORD, mysqlConfig.getUser())
                .with(MySqlConnectorConfig.SERVER_NAME, "cluster_mysql_test")
                .with(MySqlConnectorConfig.DATABASE_INCLUDE_LIST, database)
                .with(MySqlConnectorConfig.TABLE_INCLUDE_LIST, String.format("%s.%s", database, table))
                .with(MySqlConnectorConfig.SSL_MODE, mysqlConfig.getDatabaseSslMode())
                .with(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .with("database.serverTimezone", mysqlConfig.getServerTimezone())
                .with(MySqlConnectorConfig.DATABASE_HISTORY, FileDatabaseHistory.class)
                .with(FileDatabaseHistory.FILE_PATH, "/tmp/debezium/file-db-history-enum-column.txt");
    }

    private void submitRecord(KafkaConfig kafkaConfig, MySQLRecordProcessor processor) throws InterruptedException {
        // init converter and producer to send kafka data
        Converter keyConverter = RecordConverterFactory.getValueConverter(kafkaConfig, RecordConverterFactory.Type.KEY);
        Converter valueConverter = RecordConverterFactory.getValueConverter(kafkaConfig, RecordConverterFactory.Type.VALUE);
        ChangeDataProducer producer = new ChangeDataProducer(kafkaConfig, keyConverter, valueConverter);

        // poll and send data
        processor.pollData(producer::batchProduce);
        // close producer
        producer.close();
    }
}
