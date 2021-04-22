package org.apache.hudi.debezium.kafka.util;

import com.fasterxml.jackson.databind.JsonNode;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.commons.lang.StringUtils;
import org.apache.hudi.debezium.util.JsonUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class KafkaConnectUtils {

    private static final OkHttpClient client = new OkHttpClient();

    public static final String CONNECTORS_ENDPOINT = "connectors";

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
                return JsonUtils.jsonToList(Objects.requireNonNull(response.body()).string());
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
                JsonNode connectorCfg = JsonUtils.getJsonNode(Objects.requireNonNull(response.body()).string());
                return JsonUtils.jsonToMap(connectorCfg.get("config"));
            }
            throw new IOException("Unexpected code " + response);
        }
    }

}
