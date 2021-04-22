package org.apache.hudi.debezium.mysql.data;

public enum PartitionMethod {

    NONE,
    RANGE,
    LIST,
    HASH,
    KEY;
}
