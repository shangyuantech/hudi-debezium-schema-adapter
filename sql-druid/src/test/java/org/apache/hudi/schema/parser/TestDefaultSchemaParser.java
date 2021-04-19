package org.apache.hudi.schema.parser;

import org.junit.Before;
import org.junit.Test;

public class TestDefaultSchemaParser {

    private DefaultSchemaParser defaultSqlParser;

    @Before
    public void initDefaultSqlParser() {
        defaultSqlParser = new DefaultSchemaParser();
    }

    @Test
    public void testAlterAddColSql() {
        String sql = "ALTER TABLE test_database.test_table ADD test_a CHAR(1) DEFAULT 'a'";
        System.out.println(defaultSqlParser.getSqlStat(sql));
    }

    @Test
    public void testAlterChangeColSql() {
        String sql = "ALTER TABLE test_table CHANGE name name1 varchar(50) Comment 'col name'";
        System.out.println(defaultSqlParser.getSqlStat(sql));
    }

}
