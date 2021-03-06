package org.apache.hudi.debezium.mysql.data;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang.StringUtils;
import org.apache.hudi.debezium.kafka.consumer.record.SchemaRecord;

public class MySQLSchemaChange implements SchemaRecord {

    @JsonProperty("database_name")
    private String databaseName;

    private String ddl;

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getDdl() {
        return ddl;
    }

    public void setDdl(String ddl) {
        this.ddl = ddl;
    }

    private Source source;

    public Source getSource() {
        if (source == null) {
            source = new Source();
        }
        return source;
    }

    public void setSource(Source source) {
        this.source = source;
    }

    public static class Source {
        private String server;

        public String getServer() {
            return server;
        }

        public Source setServer(String server) {
            this.server = server;
            return this;
        }
    }

    private Position position;

    public Position getPosition() {
        if (position == null) {
            position = new Position();
        }
        return position;
    }

    public void setPosition(Position position) {
        this.position = position;
    }

    public static class Position {

        @JsonProperty("ts_sec")
        private Long tsSec;

        private String file;

        private Long pos;

        @JsonProperty("server_id")
        private String serverId;

        public Long getTsSec() {
            return tsSec;
        }

        public void setTsSec(Long tsSec) {
            this.tsSec = tsSec;
        }

        public String getFile() {
            return StringUtils.isBlank(file) ? "" : file;
        }

        public void setFile(String file) {
            this.file = file;
        }

        public Long getPos() {
            return pos == null ? 0L : pos;
        }

        public void setPos(Long pos) {
            this.pos = pos;
        }

        public String getServerId() {
            return serverId;
        }

        public void setServerId(String serverId) {
            this.serverId = serverId;
        }
    }
}
