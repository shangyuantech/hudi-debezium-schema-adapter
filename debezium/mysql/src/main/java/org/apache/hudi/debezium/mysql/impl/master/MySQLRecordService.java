package org.apache.hudi.debezium.mysql.impl.master;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang.StringUtils;
import org.apache.hudi.debezium.common.DBType;
import org.apache.hudi.debezium.common.TopicConfig;
import org.apache.hudi.debezium.common.exception.DebeziumConfigNotFoundException;
import org.apache.hudi.debezium.config.DebeziumConfig;
import org.apache.hudi.debezium.config.DebeziumConfigCache;
import org.apache.hudi.debezium.config.KafkaConfig;
import org.apache.hudi.debezium.kafka.connect.DebeziumConfigBuilderPrototype;
import org.apache.hudi.debezium.kafka.connect.scanner.ConnectorScanner;
import org.apache.hudi.debezium.kafka.consumer.record.IRecordService;
import org.apache.hudi.debezium.kafka.consumer.record.SchemaRecord;
import org.apache.hudi.debezium.kafka.util.AvroUtils;
import org.apache.hudi.debezium.mysql.data.MySQLDebeziumConfig;
import org.apache.hudi.debezium.mysql.data.MySQLSchemaChange;
import org.apache.hudi.debezium.mysql.data.MySQLTask;
import org.apache.hudi.debezium.mysql.data.PartitionMethod;
import org.apache.hudi.debezium.mysql.impl.connect.MySQLDebeziumConfigBuilder;
import org.apache.hudi.debezium.mysql.impl.jdbc.JDBCUtils;
import org.apache.hudi.debezium.mysql.impl.jdbc.Partition;
import org.apache.hudi.debezium.util.JsonUtils;
import org.apache.hudi.debezium.zookeeper.task.AlterField;
import org.apache.hudi.debezium.zookeeper.task.SubTask;
import org.apache.hudi.debezium.zookeeper.task.Task;
import org.apache.hudi.debezium.zookeeper.util.TaskUtils;
import org.apache.hudi.schema.common.DDLType;
import org.apache.hudi.schema.ddl.DDLStat;
import org.apache.hudi.schema.ddl.impl.AlterAddColStat;
import org.apache.hudi.schema.ddl.impl.AlterChangeColStat;
import org.apache.hudi.schema.parser.DefaultSchemaParser;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.apache.hudi.schema.common.DDLType.ALTER_ADD_COL;
import static org.apache.hudi.schema.common.DDLType.ALTER_CHANGE_COL;

public class MySQLRecordService implements IRecordService {

    private final static DefaultSchemaParser schemaParser = new DefaultSchemaParser();

    private final String topic;

    private final TopicConfig topicConfig;

    private final KafkaConfig kafkaConfig;

    private final Class<?> valueDesClass;

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
            schemaChange = JsonUtils.readValue(String.valueOf(record.value()), MySQLSchemaChange.class);
        }

        return schemaChange;
    }

    private final static List<DDLType> SUPPORTED_DDL = new ArrayList<DDLType>() {{
        add(ALTER_ADD_COL);
        add(ALTER_CHANGE_COL);
    }};

    @Override
    public Optional<Task<?>> publishTask(SchemaRecord record) throws Exception {
        String kafkaConnectUrl = topicConfig.getKafkaConnectUrl();
        MySQLDebeziumConfig mysqlConfig = (MySQLDebeziumConfig) DebeziumConfigCache.CACHE.getDefaultConfig(topic);
        String ddl = ((MySQLSchemaChange) record).getDdl();

        // check if this sql need to publish task
        DDLStat ddlStat = schemaParser.getSqlStat(ddl);
        if (!SUPPORTED_DDL.contains(ddlStat.getDdlType())) {
            if (logger.isDebugEnabled()) {
                logger.debug("[master] no need to publish ddl `{}`", ddl);
                return Optional.empty();
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

        // build task
        String database = ddlStat.getDatabase() == null ? ((MySQLSchemaChange) record).getDatabaseName() : ddlStat.getDatabase();
        String table = ddlStat.getTable();
        String taskName = TaskUtils.getTaskName(topic, database, table);
        MySQLTask task = new MySQLTask(taskName, mysqlConfig).setDdlType(ddlStat.getDdlType());

        // get jdbc link and get table schema
        List<SubTask> subTasks = buildTasks(mysqlConfig, database, ddlStat);
        task.addSubTasks(subTasks);

        if (logger.isDebugEnabled()) {
            logger.debug("[publish] create a debezium task `{}` and subTasks\n{}", taskName, task.getTasks());
        }
        return Optional.of(task);
    }

    private List<SubTask> buildTasks(MySQLDebeziumConfig mysqlConfig, String database, DDLStat ddlStat) throws SQLException {
        String table = ddlStat.getTable();
        String hostName = mysqlConfig.getHostname();
        String port = mysqlConfig.getPort();
        String user = mysqlConfig.getUser();
        String password = mysqlConfig.getPassword();

        // todo At present, SSL information is not directly configured for the time being.
        // If necessary, it can be processed in Java environment
        String databaseSslMode = mysqlConfig.getDatabaseSslMode();
        boolean ssl = false;
        switch (databaseSslMode) {
            case "verify_ca":
            case "verify_identity":
                ssl = true;
                break;
            case "disabled":
            case "preferred":
            case "required":
            default:
                break;
        }

        List<SubTask> tasks = new ArrayList<>();
        List<Partition> partitions = new ArrayList<>();
        Optional<AlterField> aField = getAlterField(ddlStat);

        // Get table partition
        try (Connection conn = JDBCUtils.createConnection(hostName, port, user, password, database, ssl)) {
            partitions.addAll(JDBCUtils.getPartitions(conn, table));
            if (logger.isDebugEnabled()) {
                logger.debug("[publish] get table {} partition \n{}", table, partitions);
            }
        }

        if (partitions.size() <= 1) {
            createSingleTask(tasks, database, table, aField, partitions.size());
        } else {
            PartitionMethod partitionMethod = partitions.get(0).getPartitionMethod();
            if (partitionMethod == null || partitionMethod == PartitionMethod.NONE) {
                createSingleTask(tasks, database, table, aField, partitions.size());
            } else {
                // todo For the time being, only support partition method
                // PartitionMethod subpartitionMethod = partitions.get(0).getSubpartitionMethod();
                switch(partitionMethod) {
                    case RANGE:
                        createPartitionRangeTask(tasks, database, table, ddlStat.getDdlType(), aField, partitions);
                        break;
                    default:
                        break;
                }
            }
        }

        return tasks;
    }

    private void createSingleTask(List<SubTask> tasks, String database, String table,
                                  Optional<AlterField> aField, Integer size) {
        if (logger.isDebugEnabled()) {
            logger.debug("[publish] Table `{}` partition size is {}, consider it a non partitioned table",
                    table, size);
        }

        String subTaskName = TaskUtils.getSubTaskName(
                DDLType.NONE.name(), aField.isPresent() ? aField.get().getFieldsDesc() : "", 0);
        SubTask subTask = new SubTask(subTaskName).setSql(String.format("select * from %s.%s", database, table));
        tasks.add(subTask.addAlterField(aField));
    }

    private void createPartitionRangeTask(List<SubTask> tasks, String database, String table, DDLType ddlType,
                                          Optional<AlterField> aField, List<Partition> partitions) {
        String partitionField = partitions.get(0).getPartitionExpression();
        int row = partitions.size();
        long before = 0;

        for (int i = 0 ; i < row ; i ++) {
            Partition partition = partitions.get(i);
            StringBuilder sql = new StringBuilder();
            sql.append(String.format("select * from %s.%s where %s",
                    database, table, partitionField));
            if (i == 0) {
                sql.append(" < ")
                        .append(partition.getPartitionDescription());
                before = Long.parseLong(partition.getPartitionDescription());
            } else if (i == row - 1) {
                sql.append(" >= ").append(before);
            } else {
                sql.append(" >= ").append(before);
                sql.append(" and ")
                        .append(partitionField)
                        .append(" < ")
                        .append(partition.getPartitionDescription());
                before = Long.parseLong(partition.getPartitionDescription());
            }

            String subTaskName = TaskUtils.getSubTaskName(
                    ddlType.name(),
                    aField.isPresent() ? aField.get().getFieldsDesc() : "",
                    i);
            SubTask subTask = new SubTask(subTaskName)
                    .setPartitionMethod(ddlType.name())
                    .setSql(sql.toString())
                    .addPartitionField(partitionField)
                    .addAlterField(aField);
            tasks.add(subTask);
        }
    }

    private Optional<AlterField> getAlterField(DDLStat ddlStat) {
        AlterField aField = null;
        if (ddlStat instanceof AlterAddColStat) {
            aField = new AlterField();
            aField.setNewName(((AlterAddColStat) ddlStat).getAddColName());
            aField.setNewType(((AlterAddColStat) ddlStat).getDataType().getName());
        } else if (ddlStat instanceof AlterChangeColStat) {
            aField = new AlterField();
            aField.setNewName(((AlterChangeColStat) ddlStat).getNewColumnName());
            aField.setOldName(((AlterChangeColStat) ddlStat).getOldColumnName());
            aField.setNewType(((AlterChangeColStat) ddlStat).getDataType().getName());
        }

        return Optional.ofNullable(aField);
    }
}
