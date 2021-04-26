package org.apache.hudi.debezium.zookeeper.task;

import org.apache.hudi.debezium.config.DebeziumConfig;
import org.apache.hudi.debezium.kafka.consumer.record.SchemaRecord;

import java.util.ArrayList;
import java.util.List;

public class Task<T extends DebeziumConfig> {

    private String name;

    private SchemaRecord schemaRecord;

    private T debeziumConfig;

    private List<SubTask> tasks = new ArrayList<>();

    public Task() {
    }

    public Task(String name, T debeziumConfig, SchemaRecord schemaRecord) {
        this.name = name;
        this.debeziumConfig = debeziumConfig;
        this.schemaRecord = schemaRecord;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setDebeziumConfig(T debeziumConfig) {
        this.debeziumConfig = debeziumConfig;
    }

    public void setTasks(List<SubTask> tasks) {
        this.tasks = tasks;
    }

    public String getName() {
        return name;
    }

    public T getDebeziumConfig() {
        return debeziumConfig;
    }

    public List<SubTask> getTasks() {
        return tasks;
    }

    public Task<T> addTask(SubTask task) {
        tasks.add(task);
        return this;
    }

    public Task<T> addSubTasks(List<SubTask> subTasks) {
        this.tasks.addAll(subTasks);
        return this;
    }

    public SchemaRecord getSchemaRecord() {
        return schemaRecord;
    }

    public void setSchemaRecord(SchemaRecord schemaRecord) {
        this.schemaRecord = schemaRecord;
    }

    @Override
    public String toString() {
        return "Task{" +
                "name='" + name + '\'' +
                ", schemaRecord=" + schemaRecord +
                ", debeziumConfig=" + debeziumConfig +
                ", tasks=" + tasks +
                '}';
    }
}
