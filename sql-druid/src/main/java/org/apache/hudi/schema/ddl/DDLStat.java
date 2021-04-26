package org.apache.hudi.schema.ddl;

import org.apache.hudi.schema.common.DDLType;

public class DDLStat {

    private String database;

    private String table;

    private String originSql;

    private DDLType ddlType;

    public DDLStat() {
    }

    public DDLStat(String database, String table, String originSql, DDLType ddlType) {
        this.database = database;
        this.table = table;
        this.originSql = originSql;
        this.ddlType = ddlType;
    }

    public String getOriginSql() {
        return originSql;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public void setDdlType(DDLType ddlType) {
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
                ", originSql='" + originSql + '\'' +
                ", ddlType=" + ddlType +
                '}';
    }
}
