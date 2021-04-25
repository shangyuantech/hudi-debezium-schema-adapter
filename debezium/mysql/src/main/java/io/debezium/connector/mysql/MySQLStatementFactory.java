package io.debezium.connector.mysql;

import io.debezium.jdbc.JdbcConnection;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class MySQLStatementFactory implements JdbcConnection.StatementFactory {

    private final Integer fetchSize;

    public MySQLStatementFactory(Integer fetchSize) {
        this.fetchSize = fetchSize;
    }

    @Override
    public Statement createStatement(Connection connection) throws SQLException {
        Statement stmt = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        stmt.setFetchSize(fetchSize);
        return stmt;
    }
}
