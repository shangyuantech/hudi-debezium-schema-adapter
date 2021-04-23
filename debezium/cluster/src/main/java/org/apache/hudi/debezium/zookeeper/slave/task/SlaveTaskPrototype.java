package org.apache.hudi.debezium.zookeeper.slave.task;

import org.apache.hudi.debezium.common.DBType;
import org.apache.hudi.debezium.common.exception.SlaveTaskNotFoundException;

import java.util.HashMap;

public class SlaveTaskPrototype {

    private final HashMap<DBType, ISlaveTask> slaveTaskPrototype = new HashMap<>();

    public void addSlaveTask(DBType dbType, ISlaveTask slaveTask) {
        slaveTaskPrototype.put(dbType, slaveTask);
    }

    public ISlaveTask getSlaveTask(DBType dbType) throws SlaveTaskNotFoundException {
        if (slaveTaskPrototype.containsKey(dbType)) {
            return slaveTaskPrototype.get(dbType);
        } else {
            throw new SlaveTaskNotFoundException(
                    String.format("can not find slave topic task instance , db_type = %s", dbType));
        }
    }
}
