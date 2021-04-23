package org.apache.hudi.debezium.mysql.impl.slave;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.hudi.debezium.common.TopicConfig;
import org.apache.hudi.debezium.config.KafkaConfig;
import org.apache.hudi.debezium.kafka.producer.ChangeDataProducer;
import org.apache.hudi.debezium.kafka.producer.record.RecordConverter;
import org.apache.hudi.debezium.kafka.producer.record.RecordConverterFactory;
import org.apache.hudi.debezium.mysql.data.MySQLDebeziumConfig;
import org.apache.hudi.debezium.mysql.data.MySQLTask;
import org.apache.hudi.debezium.mysql.jdbc.JDBCUtils;
import org.apache.hudi.debezium.util.JsonUtils;
import org.apache.hudi.debezium.zookeeper.slave.task.ISlaveTask;
import org.apache.hudi.debezium.zookeeper.task.SubTask;
import org.apache.kafka.clients.producer.ProducerRecord;
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

import static org.apache.hudi.debezium.kafka.producer.record.RecordConverterFactory.Type.KEY;
import static org.apache.hudi.debezium.kafka.producer.record.RecordConverterFactory.Type.VALUE;

public class MySQLSlaveTask implements ISlaveTask {

    private final static Logger logger = LoggerFactory.getLogger(MySQLSlaveTask.class);

    @Override
    public void eventTrigger(CuratorFramework client, TreeCacheEvent event) throws Exception {
        ChildData data = event.getData();
        String subTaskConfigStr = new String(event.getData().getData());
        SubTask subTask = JsonUtils.readValue(subTaskConfigStr, SubTask.class);
        String taskName = subTask.getName();
        if (logger.isDebugEnabled()) {
            logger.debug("[slave] [task: {}] start to run task with data = {}", taskName, subTaskConfigStr);
        }

        String mainTaskPath = Paths.get(data.getPath()).getParent().toString();
        MySQLTask mysqlTask = null;
        KafkaConfig kafkaConfig = null;
        String topicName = null;
        try {
            // get debezium config
            String mainTaskConfig = new String(client.getData().forPath(mainTaskPath));
            if (logger.isDebugEnabled()) {
                logger.debug("[slave] [task: {}] get main mysql task config = \n{}", taskName, mainTaskConfig);
            }
            mysqlTask = JsonUtils.readValue(mainTaskConfig, MySQLTask.class);

            // get kafka config
            Path topic = Paths.get(data.getPath()).getParent().getParent();
            topicName = topic.getFileName().toString();
            String topicPath = topic.toString();
            String topicConfigStr = new String(client.getData().forPath(topicPath));

            TopicConfig topicConfig = JsonUtils.readValue(topicConfigStr, TopicConfig.class);
            kafkaConfig = new KafkaConfig(topicConfig.getKafkaConfigProperties());
        } catch (KeeperException.NodeExistsException e) {
            logger.warn(String.format(
                    "[slave] [task: %s] Can not find main task config, maybe task has been cancelled!", taskName),
                    e);
            return;
        }

        MySQLDebeziumConfig mysqlConfig = mysqlTask.getDebeziumConfig();
        String database = subTask.getDatabase();
        String table = subTask.getTable();
        String hostName = mysqlConfig.getHostname();
        String port = mysqlConfig.getPort();
        String user = mysqlConfig.getUser();
        String password = mysqlConfig.getPassword();
        String databaseSslMode = mysqlConfig.getDatabaseSslMode();

        RecordConverter keyConverter = RecordConverterFactory.getValueConverter(
                kafkaConfig, KEY, topicName, database, table);
        RecordConverter valueConverter = RecordConverterFactory.getValueConverter(
                kafkaConfig, VALUE, topicName, database, table);
        ChangeDataProducer<Object, Object> consumer = new ChangeDataProducer<Object, Object>(kafkaConfig,
                keyConverter.getSerializer(), valueConverter.getSerializer());

        // get connection and pull data
        try (Connection conn = JDBCUtils.createConnection(hostName, port, user, password, database,
                JDBCUtils.checkSsl(databaseSslMode));
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(subTask.getSql())
             ) {

            ResultSetMetaData rsmd = rs.getMetaData();
            Map<String, String> typeDef = new HashMap<>();
            for (int i = 1; i <= rsmd.getColumnCount() ; i ++) {
                typeDef.put(rsmd.getColumnName(i), rsmd.getColumnTypeName(i));
            }

            // loop 1000 rows a batch
            List<ProducerRecord<Object, Object>> producerRecords = new ArrayList<>();
            int count = 0;
            while (rs.next()) {
                Map<String, Object> rowData = new HashMap<>();
                for (int i = 1; i <= rsmd.getColumnCount() ; i ++) {
                    rowData.put(rsmd.getColumnName(i), rs.getObject(i));
                }
                ProducerRecord<Object, Object> record = new ProducerRecord<>(mysqlTask.getName(),
                        valueConverter.getKey(""), valueConverter.getValue(rowData, typeDef));
                producerRecords.add(record);
                count ++;
                if (count == 1000) {
                    consumer.batchProduce(producerRecords);
                }
            }

            if (producerRecords.size() > 0) {
                consumer.batchProduce(producerRecords);
            }
        } finally {
            consumer.close();
        }
    }
}
