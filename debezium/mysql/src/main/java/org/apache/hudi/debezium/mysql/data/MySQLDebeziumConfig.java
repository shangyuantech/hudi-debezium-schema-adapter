package org.apache.hudi.debezium.mysql.data;

import org.apache.commons.lang.StringUtils;
import org.apache.hudi.debezium.config.DebeziumConfig;

import java.util.Map;

public class MySQLDebeziumConfig extends DebeziumConfig {

    public static final String DATABASE_HOSTNAME = "database.hostname";
    public static final String DATABASE_PORT = "database.port";
    public static final String DATABASE_USER = "database.user";
    public static final String DATABASE_PASSWORD = "database.password";
    public static final String DATABASE_SSL_MODE = "database.ssl.mode";
    public static final String TIME_PRECISION_MODE = "time.precision.mode";
    public static final String DATABASE_SERVER_TIMEZONE = "database.serverTimezone";

    private String hostname = "";
    private String port = "3306";
    private String user = "";
    private String password = "";
    private final static String DEFAULT_DATABASE_SSL_MODE = "disabled";
    private String databaseSslMode = DEFAULT_DATABASE_SSL_MODE;
    private String timePrecisionMode = "";
    private String serverTimezone = "";
    private final static String DEFAULT_SERVER_TIMEZONE = "Asia/Shanghai";

    public MySQLDebeziumConfig() {
    }

    public MySQLDebeziumConfig(String serverName) {
        super(serverName);
    }

    public String getHostname() {
        return hostname;
    }

    public String getPort() {
        return port;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }

    public String getTimePrecisionMode() {
        return timePrecisionMode;
    }

    public String getDatabaseSslMode() {
        return databaseSslMode == null ? DEFAULT_DATABASE_SSL_MODE : databaseSslMode;
    }

    public MySQLDebeziumConfig setHostname(String hostname) {
        this.hostname = hostname;
        return this;
    }

    public MySQLDebeziumConfig setPort(String port) {
        this.port = port;
        return this;
    }

    public MySQLDebeziumConfig setUser(String user) {
        this.user = user;
        return this;
    }

    public MySQLDebeziumConfig setPassword(String password) {
        this.password = password;
        return this;
    }

    public MySQLDebeziumConfig setDatabaseSslMode(String databaseSslMode) {
        if (StringUtils.isNotBlank(databaseSslMode))
            this.databaseSslMode = databaseSslMode;
        else this.databaseSslMode = DEFAULT_DATABASE_SSL_MODE;
        return this;
    }

    public MySQLDebeziumConfig setTimePrecisionMode(String timePrecisionMode) {
        this.timePrecisionMode = timePrecisionMode;
        return this;
    }

    public String getServerTimezone() {
        return serverTimezone;
    }

    public MySQLDebeziumConfig setServerTimezone(String serverTimezone) {
        this.serverTimezone = serverTimezone;
        return this;
    }

    @Override
    public void init(Map<String, String> configMap) {
        this.hostname = configMap.get(DATABASE_HOSTNAME);
        this.port = configMap.get(DATABASE_PORT);
        this.user = configMap.get(DATABASE_USER);
        this.password = configMap.get(DATABASE_PASSWORD);
        this.databaseSslMode = configMap.getOrDefault(DATABASE_SSL_MODE, DEFAULT_DATABASE_SSL_MODE);
        this.timePrecisionMode = configMap.get(TIME_PRECISION_MODE);
        this.serverTimezone = configMap.getOrDefault(DATABASE_SERVER_TIMEZONE, DEFAULT_SERVER_TIMEZONE);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }

        if (o instanceof MySQLDebeziumConfig) {
            return StringUtils.equals(this.hostname, ((MySQLDebeziumConfig) o).getHostname()) &&
                    StringUtils.equals(this.port, ((MySQLDebeziumConfig) o).getPort()) &&
                    StringUtils.equals(this.user, ((MySQLDebeziumConfig) o).getUser()) &&
                    StringUtils.equals(this.password, ((MySQLDebeziumConfig) o).getPassword()) &&
                    StringUtils.equals(getDatabaseSslMode(), ((MySQLDebeziumConfig) o).getDatabaseSslMode()) &&
                    StringUtils.equals(this.timePrecisionMode, ((MySQLDebeziumConfig) o).getTimePrecisionMode()) &&
                    StringUtils.equals(this.serverTimezone, ((MySQLDebeziumConfig) o).getServerTimezone()) &&
                    StringUtils.equals(this.getServerName(), ((MySQLDebeziumConfig) o).getServerName());
        }

        return false;
    }

    @Override
    public String toString() {
        return "MySQLDebeziumConfig {" +
                "serverName='" + getServerName() + '\'' +
                ", hostname='" + hostname + '\'' +
                ", port='" + port + '\'' +
                ", user='" + user + '\'' +
                ", password='" + password + '\'' +
                ", timePrecisionMode='" + timePrecisionMode + '\'' +
                "} " + super.toString();
    }
}
