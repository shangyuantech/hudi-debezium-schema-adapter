package org.apache.hudi.schema.ddl.impl;

import com.alibaba.druid.sql.ast.SQLDataType;
import com.alibaba.druid.sql.ast.SQLExpr;
import org.apache.hudi.schema.common.DDLType;
import org.apache.hudi.schema.ddl.DDLStat;

public class AlterChangeColStat extends DDLStat {

    private final String oldColumnName;

    private final String newColumnName;

    private final SQLDataType dataType;

    private final SQLExpr defaultValue;

    public AlterChangeColStat(String database, String table, DDLType ddlType,
                              String oldColumnName, String newColumnName, SQLDataType dataType, SQLExpr defaultValue) {
        super(database, table, ddlType);
        this.oldColumnName = oldColumnName;
        this.newColumnName = newColumnName;
        this.dataType = dataType;
        this.defaultValue = defaultValue;
    }

    public String getOldColumnName() {
        return oldColumnName;
    }

    public String getNewColumnName() {
        return newColumnName;
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
                ", oldColumnName='" + oldColumnName + '\'' +
                ", newColumnName='" + newColumnName + '\'' +
                ", dataType=" + dataType +
                ", defaultValue=" + defaultValue +
                " } ";
    }
}
