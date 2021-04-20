package org.apache.hudi.debezium.kafka.master.task;

import org.apache.hudi.debezium.common.DBType;
import org.apache.hudi.debezium.common.exception.DebeziumTaskNotFoundException;

import java.util.HashMap;

public class DebeziumTopicTaskPrototype {

    private final HashMap<DBType, IDebeziumTopicTask> debeziumTopicTaskPrototype = new HashMap<>();

    public DebeziumTopicTaskPrototype() {

    }

    public void addDebeziumTopicTask(DBType dbType, IDebeziumTopicTask debeziumTopicTask) {
        debeziumTopicTaskPrototype.put(dbType, debeziumTopicTask);
    }

    public IDebeziumTopicTask getTopicTask(DBType dbType) throws DebeziumTaskNotFoundException {
        if (debeziumTopicTaskPrototype.containsKey(dbType)) {
            return debeziumTopicTaskPrototype.get(dbType).newInstance();
        } else {
            throw new DebeziumTaskNotFoundException(
                    String.format("can not find debezium topic task instance , db_type = %s", dbType));
        }
    }

}
