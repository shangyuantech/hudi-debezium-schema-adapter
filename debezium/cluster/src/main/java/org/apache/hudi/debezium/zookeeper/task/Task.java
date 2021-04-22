package org.apache.hudi.debezium.zookeeper.task;

import org.apache.hudi.debezium.config.DebeziumConfig;

import java.util.ArrayList;
import java.util.List;

public class Task<T extends DebeziumConfig> {

    private String name;

    private T debeziumConfig;

    private List<SubTask> tasks = new ArrayList<>();

    public Task() {
    }

    public Task(String name, T debeziumConfig) {
        this.name = name;
        this.debeziumConfig = debeziumConfig;
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

    public Task addTask(SubTask task) {
        tasks.add(task);
        return this;
    }

    @Override
    public String toString() {
        return "Task{" +
                "name='" + name + '\'' +
                ", debeziumConfig=" + debeziumConfig +
                ", tasks=" + tasks +
                '}';
    }
}
