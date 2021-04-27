package org.apache.hudi.schema.ddl.impl;

import com.alibaba.druid.sql.ast.SQLDataType;
import com.alibaba.druid.sql.ast.SQLExpr;
import org.apache.hudi.schema.common.DDLType;
import org.apache.hudi.schema.ddl.DDLStat;

public class AlterAddColStat extends DDLStat {

    private String addColName;

    private SQLDataType dataType;

    private SQLExpr defaultValue;

    public AlterAddColStat(String database, String table, String sql, DDLType ddlType,
                           String addColName, SQLDataType dataType, SQLExpr defaultValue) {
        super(database, table, sql, ddlType);
        this.addColName = addColName;
        this.dataType = dataType;
        this.defaultValue = defaultValue;
    }

    public void setAddColName(String addColName) {
        this.addColName = addColName;
    }

    public void setDataType(SQLDataType dataType) {
        this.dataType = dataType;
    }

    public void setDefaultValue(SQLExpr defaultValue) {
        this.defaultValue = defaultValue;
    }

    public String getAddColName() {
        return addColName;
    }

    public SQLDataType getDataType() {
        return dataType;
    }

    public SQLExpr getDefaultValue() {
        return defaultValue;
    }

    @Override
    public String toString() {
        return "AlterAddColStat { " +
                "database='" + getDatabase() + '\'' +
                ", table='" + getTable() + '\'' +
                ", ddlType=" + getDdlType() +
                ", addColName='" + addColName + '\'' +
                ", dataType=" + dataType +
                ", defaultValue=" + defaultValue +
                " } ";
    }
}
