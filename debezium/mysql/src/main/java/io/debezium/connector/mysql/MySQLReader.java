package io.debezium.connector.mysql;

import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import io.debezium.function.BufferedBlockingConsumer;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import io.debezium.util.Strings;
import io.debezium.util.Threads;
import org.apache.commons.lang.StringUtils;
import org.apache.hudi.debezium.mysql.jdbc.JDBCUtils;
import org.apache.hudi.schema.common.DDLType;
import org.apache.hudi.schema.ddl.DDLStat;
import org.apache.hudi.schema.ddl.impl.AlterAddColStat;
import org.apache.hudi.schema.ddl.impl.AlterChangeColStat;
import org.apache.hudi.schema.ddl.impl.CreateTableStat;
import org.apache.hudi.schema.parser.DefaultSchemaParser;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class MySQLReader extends AbstractReader {

    private final static Logger logger = LoggerFactory.getLogger(MySQLReader.class);

    private ExecutorService executorService;

    private final String sql;

    private final TableId tableId;

    private final DDLStat ddlStat;

    public MySQLReader(String database, String table, String sql, MySqlTaskContext context, DDLStat ddlStat) {
        super(String.format("%s_%s_task", database, table), context, null);
        this.tableId = new TableId(database, null, table);
        this.sql = sql;
        this.ddlStat = ddlStat;
    }

    public MySqlTaskContext getContext() {
        return context;
    }

    public MySqlSchema getDbSchema() {
        return context.dbSchema();
    }

    @Override
    protected void doStart() {
        executorService = Threads.newSingleThreadExecutor(MySqlConnector.class,
                context.getConnectorConfig().getLogicalName(), "snapshot");
        executorService.execute(this::execute);
    }

    @Override
    protected void doStop() {
        logger.debug("Stopping snapshot reader");
        cleanupResources();
    }

    @Override
    protected void doCleanup() {
        executorService.shutdown();
        logger.debug("Completed writing all snapshot records");
    }

    protected void execute() {
        BufferedBlockingConsumer<SourceRecord> bufferedRecordQueue = BufferedBlockingConsumer.bufferLast(super::enqueueRecord);
        //context.source().setBinlogStartPoint("mysql-bin.00001", 0);

        final MySqlSchema schema = getDbSchema();
        final JdbcConnection mysql = connectionContext.jdbc();
        //DDLType ddlType = ddlStat.getDdlType();

        try {
            mysql.query("SHOW CREATE TABLE " + quote(tableId), rs -> {
                if (rs.next()) {
                    String createSQL = rs.getString(2);
                    schema.applyDdl(context.source(), tableId.catalog(),
                            //ddlType.equals(DDLType.ALTER_CHANGE_COL) ? replaceColName(createSQL) : createSQL,
                            createSQL,
                            this::enqueueSchemaChanges);
                }
            });

            RecordMakers recordMakers = context.makeRecord();
            recordMakers.assign(0, tableId);
            RecordMakers.RecordsForTable recordMaker = recordMakers.forTable(
                    tableId, null, bufferedRecordQueue);

            AtomicLong rowNum = new AtomicLong();
            final Clock clock = context.getClock();
            long start = clock.currentTimeInMillis();

            MySQLStatementFactory factory = new MySQLStatementFactory(
                    context.getConnectorConfig().getSnapshotFetchSize());

            mysql.executeWithoutCommitting("USE " + quote(tableId.catalog()) + ";");

            // get total rows
            AtomicLong numRows = new AtomicLong(-1);
            AtomicReference<String> rowCountStr = new AtomicReference<>("<unknown>");
            try {
                mysql.query("SHOW TABLE STATUS LIKE '" + tableId.table() + "';", rs -> {
                    if (rs.next()) {
                        numRows.set(rs.getLong(5));
                    }
                });
                rowCountStr.set(numRows.toString());
            } catch (SQLException e) {
                // Log it, but otherwise just use large result set by default ...
                logger.debug("Error while getting number of rows in table {}: {}", tableId, e.getMessage(), e);
            }

            // query data and send to queue
            mysql.query(StringUtils.isNotBlank(sql) ? sql : getSelect(tableId), factory, rs -> {
                final Table table = schema.tableFor(tableId);
                final int numColumns = table.columns().size();
                final Object[] after = new Object[numColumns];
                final Object[] before = new Object[numColumns];

                while (rs.next()) {
                    for (int i = 0, j = 1; i != numColumns; ++i, ++j) {
                        Column actualColumn = table.columns().get(i);
                        after[i] = JDBCUtils.readField(rs, j, actualColumn, table);
                        if (!checkIgnoreColumn(actualColumn.name())) {
                            before[i] = after[i];
                        }
                    }

                    try {
                        recordMaker.update(before, after, clock.currentTimeAsInstant());
                        rowNum.incrementAndGet();
                        // Thread.sleep(TimeUnit.SECONDS.toMillis(1));// for test thread

                        if (rowNum.get() % 1000 == 0) {
                            if (logger.isInfoEnabled()) {
                                logger.info("{} of {} rows scanned from table '{}' after {}",
                                        rowNum, rowCountStr, tableId,
                                        Strings.duration(clock.currentTimeInMillis() - start));
                            }
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

                try {
                    logger.info("Completed scanning a total of {} rows from table '{}' after {}",
                            rowNum, tableId,
                            Strings.duration(clock.currentTimeInMillis() - start));
                    bufferedRecordQueue.close(i -> i);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            });
        } catch (Exception e) {
            logger.error("error when executing task", e);
        } finally {
            completeSuccessfully();
        }
    }

    private boolean checkIgnoreColumn(String colName) {
        DDLType ddlType = ddlStat.getDdlType();

        if (ddlType.equals(DDLType.ALTER_ADD_COL) &&
                colName.equals(((AlterAddColStat) ddlStat).getAddColName())) {
            logger.trace("skip column {} value by {}", colName, ddlType);
            return true;
        } else if (ddlType.equals(DDLType.ALTER_CHANGE_COL)
                && colName.equals(((AlterChangeColStat) ddlStat).getNewColumnName())) {
            logger.trace("skip column {} value by {}. " +
                    "The ignored logic schema is consistent with debezium mysql", colName, ddlType);
            return true;
        }

        return false;
    }

    @Deprecated
    private String replaceColName(String createSQL) {
        DefaultSchemaParser sqlParser = new DefaultSchemaParser();
        CreateTableStat createStat = (CreateTableStat) sqlParser.getSqlStat(createSQL);
        SQLStatement stmt = createStat.getStmt();
        AlterChangeColStat changeColStat = (AlterChangeColStat) ddlStat;

        for (SQLObject sqlObj : stmt.getChildren()) {
            if (sqlObj instanceof SQLColumnDefinition) {
                SQLColumnDefinition colDef = (SQLColumnDefinition) sqlObj;
                if (changeColStat.getNewColumnName().equals(
                        colDef.getColumnName().replaceAll("`", ""))) {
                    colDef.setName(changeColStat.getOldColumnName());
                }
            }
        }

        StringBuilder sql = new StringBuilder();
        sql.append(stmt.toString()).append(";\n");
        sql.append(changeColStat.getOriginSql()).append(";");

        return sql.toString();
    }

    public String getSelect(TableId tableId) {
        Map<TableId, String> selectOverrides = context.getConnectorConfig().getSnapshotSelectOverridesByTable();
        return selectOverrides.getOrDefault(tableId, "SELECT * FROM " + quote(tableId));
    }

    private String quote(String dbOrTableName) {
        return "`" + dbOrTableName + "`";
    }

    private String quote(TableId id) {
        return quote(id.catalog()) + "." + quote(id.table());
    }

    public void enqueueSchemaChanges(String dbName, Set<TableId> tables, String ddlStatement) {
        // todo
    }
}
