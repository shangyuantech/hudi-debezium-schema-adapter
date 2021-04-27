package org.apache.hudi.debezium.reflection;

import org.apache.hudi.debezium.common.DBType;
import org.apache.hudi.debezium.common.exception.DebeziumConfigBuilderNotFoundException;
import org.apache.hudi.debezium.common.exception.DebeziumTaskNotFoundException;
import org.apache.hudi.debezium.common.exception.SlaveTaskNotFoundException;
import org.apache.hudi.debezium.kafka.connect.DebeziumConfigBuilderPrototype;
import org.apache.hudi.debezium.kafka.master.task.DebeziumTopicTaskPrototype;
import org.apache.hudi.debezium.zookeeper.slave.task.SlaveTaskPrototype;
import org.junit.Assert;
import org.junit.Test;

public class TestReflection {

    private final ReflectionService reflection = new ReflectionService("org.apache.hudi.debezium");

    @Test
    public void testDbzTopicTask() throws InstantiationException, IllegalAccessException,
            DebeziumTaskNotFoundException {
        DebeziumTopicTaskPrototype dbzTopicTaskPrototype = reflection.getDbzTopicTaskPrototype();
        Assert.assertNotNull(dbzTopicTaskPrototype);
        Assert.assertNotNull(dbzTopicTaskPrototype.getTopicTask(DBType.None));
    }

    @Test
    public void testDbzConfigBuilder() throws InstantiationException, IllegalAccessException,
            DebeziumConfigBuilderNotFoundException {
        DebeziumConfigBuilderPrototype dbzConfigBuilderPrototype = reflection.getDbzConfigBuilderPrototype();
        Assert.assertNotNull(dbzConfigBuilderPrototype);
        Assert.assertNotNull(dbzConfigBuilderPrototype.getDebeziumConfigBuilder(DBType.None));
    }

    @Test
    public void testSlaveTask() throws InstantiationException, IllegalAccessException, SlaveTaskNotFoundException {
        SlaveTaskPrototype slaveTaskPrototype = reflection.getSlaveTaskPrototype();
        Assert.assertNotNull(slaveTaskPrototype);
        Assert.assertNotNull(slaveTaskPrototype.getSlaveTask(DBType.None));
    }
}
