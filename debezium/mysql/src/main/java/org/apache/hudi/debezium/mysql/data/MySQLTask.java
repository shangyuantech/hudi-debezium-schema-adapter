package org.apache.hudi.debezium.mysql.data;

import org.apache.hudi.debezium.zookeeper.task.Task;
import org.apache.hudi.schema.common.DDLType;

public class MySQLTask extends Task<MySQLDebeziumConfig, MySQLSchemaChange> {

    private DDLType ddlType;

    public MySQLTask() {
    }

    public MySQLTask(String name, MySQLDebeziumConfig debeziumConfig, MySQLSchemaChange record) {
        super(name, debeziumConfig, record);
    }

    public DDLType getDdlType() {
        return ddlType;
    }

    public MySQLTask setDdlType(DDLType ddlType) {
        this.ddlType = ddlType;
        return this;
    }

    @Override
    public String toString() {
        return "MySQLTask{" +
                "ddlType=" + ddlType +
                "} " + super.toString();
    }
}
