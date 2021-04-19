package org.apache.hudi.schema.ddl;

import org.apache.hudi.schema.common.DDLType;

public class DDLStat {

    private final String database;

    private final String table;

    private final DDLType ddlType;

    public DDLStat(String database, String table, DDLType ddlType) {
        this.database = database;
        this.table = table;
        this.ddlType = ddlType;
    }

    public String getDatabase() {
        return database;
    }

    public String getTable() {
        return table;
    }

    public DDLType getDdlType() {
        return ddlType;
    }

    @Override
    public String toString() {
        return "DDLStat{" +
                "database='" + database + '\'' +
                ", table='" + table + '\'' +
                ", ddlType=" + ddlType +
                '}';
    }
}
