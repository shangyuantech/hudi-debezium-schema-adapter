package org.apache.hudi.debezium.common;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class TopicConfig {

    @JsonProperty("db_type")
    private DBType dbType;

    @JsonProperty("kafka_config")
    private Map<String, String> kafkaConfig;

    @JsonProperty("kafka_connect_url")
    private String kafkaConnectUrl;

    public DBType getDbType() {
        return dbType;
    }

    public TopicConfig setDbType(DBType dbType) {
        this.dbType = dbType;
        return this;
    }

    public Map<String, String> getKafkaConfig() {
        return kafkaConfig;
    }

    public String getKafkaConnectUrl() {
        return kafkaConnectUrl;
    }

    public TopicConfig setKafkaConnectUrl(String kafkaConnectUrl) {
        this.kafkaConnectUrl = kafkaConnectUrl;
        return this;
    }

    public TopicConfig addKafkaConfig(String key, String value) {
        if (kafkaConfig == null) kafkaConfig = new HashMap<>();
        this.kafkaConfig.put(key, value);
        return this;
    }

    public void addKafkaConfig(Map<String, String> properties) {
        if (kafkaConfig == null) kafkaConfig = new HashMap<>();
        this.kafkaConfig.putAll(properties);
    }

    public void setKafkaConfig(Map<String, String> kafkaConfig) {
        this.kafkaConfig = kafkaConfig;
    }

    @JsonIgnore
    public Properties getKafkaConfigProperties() {
        Properties properties = new Properties();
        properties.putAll(kafkaConfig);
        return properties;
    }
}
