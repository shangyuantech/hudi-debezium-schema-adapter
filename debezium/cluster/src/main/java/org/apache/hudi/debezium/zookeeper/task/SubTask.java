package org.apache.hudi.debezium.zookeeper.task;

public class SubTask {

    private String name;

    private String sql;

    public SubTask() {
    }

    public SubTask(String name) {
        this.name = name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public String getSql() {
        return sql;
    }

    public SubTask setSql(String sql) {
        this.sql = sql;
        return this;
    }

    @Override
    public String toString() {
        return "SubTask{" +
                "name='" + name + '\'' +
                ", sql='" + sql + '\'' +
                '}';
    }
}
