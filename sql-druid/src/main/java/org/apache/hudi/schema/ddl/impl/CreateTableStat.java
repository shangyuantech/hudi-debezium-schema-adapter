package org.apache.hudi.schema.ddl.impl;

import com.alibaba.druid.sql.ast.SQLStatement;
import org.apache.hudi.schema.common.DDLType;
import org.apache.hudi.schema.ddl.DDLStat;

public class CreateTableStat extends DDLStat {

    private final SQLStatement stmt;

    public CreateTableStat(String database, String table, String sql, DDLType ddlType, SQLStatement stmt) {
        super(database, table, sql, ddlType);
        this.stmt = stmt;
    }

    public SQLStatement getStmt() {
        return stmt;
    }

    @Override
    public String toString() {
        return stmt.toString();
    }
}
