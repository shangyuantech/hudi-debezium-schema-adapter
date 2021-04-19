package org.apache.hudi.schema.ddl.impl;

import org.apache.hudi.schema.common.DDLType;
import org.apache.hudi.schema.ddl.DDLStat;

public class NoneStat extends DDLStat {

    private final static NoneStat noneStat = new NoneStat(null, null, DDLType.NONE);

    public NoneStat(String database, String table, DDLType ddlType) {
        super(database, table, ddlType);
    }

    public static NoneStat noneStat() {
        return noneStat;
    }

    @Override
    public String toString() {
        return "NoneStat: can not find ddl stat by sql";
    }
}
