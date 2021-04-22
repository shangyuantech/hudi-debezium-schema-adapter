package org.apache.hudi.debezium.mysql.data;

import org.apache.hudi.debezium.zookeeper.task.Task;
import org.apache.hudi.schema.common.DDLType;

public class MySQLTask extends Task<MySQLDebeziumConfig> {

    private DDLType ddlType;

    public MySQLTask() {
    }

    public MySQLTask(String name, MySQLDebeziumConfig debeziumConfig) {
        super(name, debeziumConfig);
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
