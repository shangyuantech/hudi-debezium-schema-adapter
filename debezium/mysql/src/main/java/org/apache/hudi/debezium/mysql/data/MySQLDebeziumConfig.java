package org.apache.hudi.debezium.mysql.data;

import org.apache.hudi.debezium.kafka.config.DebeziumConfig;

import java.util.Map;

public class MySQLDebeziumConfig extends DebeziumConfig {

    public static final String DATABASE_HOSTNAME = "database.hostname";
    public static final String DATABASE_PORT = "database.port";
    public static final String DATABASE_USER = "database.user";
    public static final String DATABASE_PASSWORD = "database.password";
    public static final String TIME_PRECISION_MODE = "time.precision.mode";

    private String hostname = "";
    private String port = "";
    private String user = "";
    private String password = "";
    private String timePrecisionMode = "";

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

    @Override
    public void init(Map<String, String> configMap) {
        this.hostname = configMap.get(DATABASE_HOSTNAME);
        this.port = configMap.get(DATABASE_PORT);
        this.user = configMap.get(DATABASE_USER);
        this.password = configMap.get(DATABASE_PASSWORD);
        this.timePrecisionMode = configMap.get(TIME_PRECISION_MODE);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }

        if (o instanceof MySQLDebeziumConfig) {
            return this.hostname.equals(((MySQLDebeziumConfig) o).getHostname()) &&
                    this.port.equals(((MySQLDebeziumConfig) o).getPort()) &&
                    this.user.equals(((MySQLDebeziumConfig) o).getUser()) &&
                    this.password.equals(((MySQLDebeziumConfig) o).getPassword()) &&
                    this.timePrecisionMode.equals(((MySQLDebeziumConfig) o).getTimePrecisionMode()) &&
                    this.getServerName().equals(((MySQLDebeziumConfig) o).getServerName());
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
