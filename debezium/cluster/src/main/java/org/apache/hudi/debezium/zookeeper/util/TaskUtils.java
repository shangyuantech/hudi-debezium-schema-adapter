package org.apache.hudi.debezium.zookeeper.util;

import org.apache.commons.lang.StringUtils;

public class TaskUtils {

    public static String getTaskName(String serverName, String database, String table) {
        if (StringUtils.isBlank(database)) {
            return String.format("%s.%s", serverName, table);
        } else {
            return String.format("%s.%s.%s", serverName, database, table);
        }
    }

    public static String getSubTaskName(String operation, String fieldsDesc, int order) {
        return String.format("%s#%s#%s", operation, fieldsDesc, order);
    }
}
