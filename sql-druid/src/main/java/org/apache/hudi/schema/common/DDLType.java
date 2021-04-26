package org.apache.hudi.schema.common;

public enum DDLType {

    NONE,
    CREATE_TABLE,
    ALTER_ADD_COL,
    ALTER_CHANGE_COL,
    OTHER;

}
