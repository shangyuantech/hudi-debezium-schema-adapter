package org.apache.hudi.schema.parser;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLDataType;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableAddColumn;
import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableChangeColumn;
import com.alibaba.druid.util.JdbcConstants;
import org.apache.commons.lang.StringUtils;
import org.apache.hudi.schema.ddl.DDLStat;
import org.apache.hudi.schema.ddl.impl.AlterAddColStat;
import org.apache.hudi.schema.ddl.impl.AlterChangeColStat;
import org.apache.hudi.schema.ddl.impl.CreateTableStat;
import org.apache.hudi.schema.ddl.impl.NoneStat;

import java.util.List;
import java.util.Optional;

import static org.apache.hudi.schema.common.DDLType.*;

/**
 * Default schema parser is based on mysql
 */
public class DefaultSchemaParser {

    final private DbType dbType = JdbcConstants.MYSQL;

    public DbType getDbType() {
        return dbType;
    }

    public DefaultSchemaParser() {
    }

    public DDLStat getSqlStat(String sql) {
        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, dbType);
        SQLStatement stmt = stmtList.get(0);

        // 1st loop for database and table name
        String database = null;
        String table = null;
        for (SQLObject sqlObject : stmt.getChildren()) {
            if (sqlObject instanceof SQLExprTableSource) {
                database = ((SQLExprTableSource) sqlObject).getSchema();
                table = ((SQLExprTableSource) sqlObject).getTableName();
            }
        }
        if (StringUtils.isBlank(table)) {
            return NoneStat.noneStat();
        }

        Optional<DDLStat> customStat = createCustomStat(database, table, sql, stmt.getChildren());
        if (customStat.isPresent()) {
            return customStat.get();
        } else {
            return createDDLStat(database, table, sql, stmt);
        }
    }

    protected DDLStat createDDLStat(String database, String table, String sql, SQLStatement stmt) {
        // 2nd loop for infected columns
        for (SQLObject sqlObj : stmt.getChildren()) {
            if (sqlObj instanceof SQLAlterTableAddColumn) {
                return createAlterAddStat(database, table, sql, ((SQLAlterTableAddColumn) sqlObj));
            } else if (sqlObj instanceof MySqlAlterTableChangeColumn) {
                return createAlterChangeStat(database, table, sql, ((MySqlAlterTableChangeColumn) sqlObj));
            } else if (sqlObj instanceof SQLColumnDefinition) {
                return createCreateTableStat(database, table, sql, stmt);
            }
        }

        return NoneStat.noneStat();
    }

    /**
     * Provides a custom implementation
     */
    protected Optional<DDLStat> createCustomStat(String database, String table, String sql, List<SQLObject> sqlObjs) {
        return Optional.empty();
    }

    protected DDLStat createAlterAddStat(String database, String table, String sql, SQLAlterTableAddColumn sqlObj) {
        SQLColumnDefinition sqlColDef = sqlObj.getColumns().get(0);
        String columnName = sqlColDef.getColumnName();
        SQLDataType dataType = sqlColDef.getDataType();
        SQLExpr defaultExpr = sqlColDef.getDefaultExpr();
        return new AlterAddColStat(database, table, sql, ALTER_ADD_COL, columnName, dataType, defaultExpr);
    }

    protected DDLStat createAlterChangeStat(String database, String table, String sql, MySqlAlterTableChangeColumn sqlObj) {
        String columnName = sqlObj.getColumnName().getSimpleName();
        SQLColumnDefinition sqlColDef = sqlObj.getNewColumnDefinition();
        SQLDataType dataType = sqlColDef.getDataType();
        SQLExpr defaultExpr = sqlColDef.getDefaultExpr();
        return new AlterChangeColStat(database, table, sql, ALTER_CHANGE_COL, columnName,
                sqlColDef.getColumnName(), dataType, defaultExpr);
    }

    protected DDLStat createCreateTableStat(String database, String table, String sql, SQLStatement stmt) {
        return new CreateTableStat(database, table, sql, CREATE_TABLE, stmt);
    }
}
