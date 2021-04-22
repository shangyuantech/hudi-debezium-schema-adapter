package org.apache.hudi.debezium.config;

import java.util.Map;
import java.util.Properties;

public class KafkaConfig {

    private final Properties props = new Properties();

    public KafkaConfig() {
        setEnvProperties();
    }

    public KafkaConfig(Properties props) {
        this.props.putAll(props);
        setEnvProperties();
    }

    private void setEnvProperties() {
        Map<String, String> envs = System.getenv();
        envs.entrySet().stream()
                .filter(entry -> entry.getKey().startsWith("KAFKA")).forEach(entry -> {
            String propsKey = entry.getKey().toLowerCase().substring(6)
                    .replaceAll("_", ".")
                    .replaceAll("__", "-")
                    .replaceAll("___", "_");
            props.put(propsKey, entry.getValue());
        });
    }

    public Properties getProps() {
        return props;
    }

    public String get(String key) {
        return String.valueOf(props.get(key));
    }

    public String getOrDefault(String key, String defaultValue) {
        return String.valueOf(props.getOrDefault(key, defaultValue));
    }

}
