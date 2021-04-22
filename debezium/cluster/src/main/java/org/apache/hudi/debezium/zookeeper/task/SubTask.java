package org.apache.hudi.debezium.zookeeper.task;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class SubTask {

    private final static String DEFAULT_PARTITION_METHOD = "NONE";

    private String name;

    private String sql;

    private String partitionMethod = DEFAULT_PARTITION_METHOD;

    private List<String> partitionFields = new ArrayList<>();

    private List<AlterField> alterFields = new ArrayList<>();

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

    public String getPartitionMethod() {
        return partitionMethod;
    }

    public SubTask setPartitionMethod(String partitionMethod) {
        this.partitionMethod = partitionMethod;
        return this;
    }

    public List<String> getPartitionFields() {
        return partitionFields;
    }

    public void setPartitionFields(List<String> partitionFields) {
        this.partitionFields = partitionFields;
    }

    public SubTask addPartitionField(String fieldName) {
        partitionFields.add(fieldName);
        return this;
    }

    public List<AlterField> getAlterFields() {
        return alterFields;
    }

    public void setAlterFields(List<AlterField> alterFields) {
        this.alterFields = alterFields;
    }

    public SubTask addAlterField(Optional<AlterField> alterField) {
        alterField.ifPresent(field -> alterFields.add(field));
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
