package org.apache.hudi.debezium.reflection;

import org.apache.hudi.debezium.kafka.connect.DebeziumConfigBuilderPrototype;
import org.apache.hudi.debezium.kafka.connect.IDebeziumConfigBuilder;
import org.apache.hudi.debezium.kafka.master.task.DebeziumTopicTaskPrototype;
import org.apache.hudi.debezium.kafka.master.task.IDebeziumTopicTask;
import org.apache.hudi.debezium.zookeeper.slave.task.ISlaveTask;
import org.apache.hudi.debezium.zookeeper.slave.task.SlaveTaskPrototype;
import org.reflections.Reflections;

import java.util.Set;

public class ReflectionService {

    private final String packageName;

    private final Reflections reflections;

    public ReflectionService(String packageName) {
        this.packageName = packageName;
        this.reflections = new Reflections(packageName);
    }

    public DebeziumTopicTaskPrototype getDbzTopicTaskPrototype()
            throws IllegalAccessException, InstantiationException {
        Set<Class<? extends IDebeziumTopicTask>> subTypes =
                reflections.getSubTypesOf(IDebeziumTopicTask.class);
        DebeziumTopicTaskPrototype dbzTopicTaskPrototype =
                new DebeziumTopicTaskPrototype();

        for (Class<? extends IDebeziumTopicTask> clazz : subTypes) {
            IDebeziumTopicTask task = clazz.newInstance();
            dbzTopicTaskPrototype.addDebeziumTopicTask(task.getDbType(), task);
        }

        return dbzTopicTaskPrototype;
    }

    public DebeziumConfigBuilderPrototype getDbzConfigBuilderPrototype()
            throws IllegalAccessException, InstantiationException {
        Set<Class<? extends IDebeziumConfigBuilder>> subTypes =
                reflections.getSubTypesOf(IDebeziumConfigBuilder.class);
        DebeziumConfigBuilderPrototype dbzConfigBuilderPrototype =
                new DebeziumConfigBuilderPrototype();

        for (Class<? extends IDebeziumConfigBuilder> clazz : subTypes) {
            IDebeziumConfigBuilder configBuilder = clazz.newInstance();
            dbzConfigBuilderPrototype.addDebeziumConfigBuilder(configBuilder.getDbType(), configBuilder);
        }

        return dbzConfigBuilderPrototype;
    }

    public SlaveTaskPrototype getSlaveTaskPrototype() throws IllegalAccessException, InstantiationException {
        Set<Class<? extends ISlaveTask>> subTypes = reflections.getSubTypesOf(ISlaveTask.class);
        SlaveTaskPrototype slaveTaskPrototype = new SlaveTaskPrototype();

        for (Class<? extends ISlaveTask> clazz : subTypes) {
            ISlaveTask slaveTask = clazz.newInstance();
            slaveTaskPrototype.addSlaveTask(slaveTask.getDbType(), slaveTask);
        }

        return slaveTaskPrototype;
    }

}
