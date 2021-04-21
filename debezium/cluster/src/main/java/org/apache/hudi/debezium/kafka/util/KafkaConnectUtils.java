package org.apache.hudi.debezium.kafka.util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class KafkaConnectUtils {

    private static final OkHttpClient client = new OkHttpClient();

    public static final String CONNECTORS_ENDPOINT = "connectors";

    private static final ObjectMapper objectMapper = new ObjectMapper();

    private static String combineUrls(String kafkaConnectUrl, String ... endpoints) {
        return String.format("%s/%s",
                kafkaConnectUrl.endsWith("/") ? kafkaConnectUrl.substring(0, kafkaConnectUrl.length() - 1) : kafkaConnectUrl,
                endpoints.length > 1 ? StringUtils.join(endpoints, "/") : endpoints[0]);
    }

    /**
     * find connectors list
     */
    public static List<String> getConnectors(String kafkaConnectUrl) throws IOException {
        Request request = new Request.Builder()
                .url(combineUrls(kafkaConnectUrl, CONNECTORS_ENDPOINT))
                .build();

        try (Response response = client.newCall(request).execute()) {
            if (response.isSuccessful()) {
                return objectMapper.readValue(Objects.requireNonNull(response.body()).string(),
                        new TypeReference<List<String>>(){});
            }
            throw new IOException("Unexpected code " + response);
        }
    }

    /**
     * get connector config map
     */
    public static Map<String, String> getConnectorConfig(String kafkaConnectUrl, String connector) throws IOException {
        Request request = new Request.Builder()
                .url(combineUrls(kafkaConnectUrl, CONNECTORS_ENDPOINT, connector))
                .build();

        try (Response response = client.newCall(request).execute()) {
            if (response.isSuccessful()) {
                JsonNode connectorCfg = objectMapper.readTree(Objects.requireNonNull(response.body()).string());
                return objectMapper.readValue(objectMapper.treeAsTokens(connectorCfg.get("config")),
                        new TypeReference<Map<String, String>>(){});
            }
            throw new IOException("Unexpected code " + response);
        }
    }

}
