package org.apache.hudi.schema.parser;

import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import org.apache.hudi.schema.ddl.DDLStat;
import org.apache.hudi.schema.ddl.impl.AlterAddColStat;
import org.apache.hudi.schema.ddl.impl.AlterChangeColStat;
import org.apache.hudi.schema.ddl.impl.CreateTableStat;
import org.apache.hudi.schema.ddl.impl.NoneStat;
import org.junit.Assert;
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
        DDLStat testStat = defaultSqlParser.getSqlStat(sql);
        System.out.println(testStat);
        Assert.assertTrue(testStat instanceof AlterAddColStat);
    }

    @Test
    public void testAlterChangeColSql() {
        String sql = "ALTER TABLE test_table CHANGE name name1 varchar(50) Comment 'col name'";
        DDLStat testStat = defaultSqlParser.getSqlStat(sql);
        System.out.println(testStat);
        Assert.assertTrue(testStat instanceof AlterChangeColStat);
    }

    @Test
    public void testNone() {
        String sql = "/* ApplicationName=DBeaver 21.0.1 - SQLEditor <Script-74.sql> */ create table test (id int(11), name varchar(50))";
        DDLStat testStat = defaultSqlParser.getSqlStat(sql);
        System.out.println(testStat);
        Assert.assertTrue(testStat instanceof CreateTableStat);

        CreateTableStat create = (CreateTableStat) testStat;
        for (SQLObject sqlObj : create.getStmt().getChildren()) {
            if (sqlObj instanceof SQLColumnDefinition) {
                SQLColumnDefinition colDef = (SQLColumnDefinition) sqlObj;
                if (colDef.getColumnName().equals("name")) {
                    colDef.setName("name_change");
                }
            }
        }
        System.out.println(create.toString());
    }

}
