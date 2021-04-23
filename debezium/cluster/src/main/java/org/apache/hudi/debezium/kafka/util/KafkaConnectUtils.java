package org.apache.hudi.debezium.kafka.util;

import com.fasterxml.jackson.databind.JsonNode;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.avro.Schema;
import org.apache.commons.lang.StringUtils;
import org.apache.hudi.debezium.util.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class KafkaConnectUtils {

    private final static Logger logger = LoggerFactory.getLogger(KafkaConnectUtils.class);

    private static final OkHttpClient client = new OkHttpClient();

    public static final String CONNECTORS_ENDPOINT = "connectors";

    private static String combineUrls(String url, String ... endpoints) {
        return String.format("%s/%s",
                url.endsWith("/") ? url.substring(0, url.length() - 1) : url,
                endpoints.length > 1 ? StringUtils.join(endpoints, "/") : endpoints[0]);
    }

    /**
     * find connectors list
     */
    public static List<String> getConnectors(String kafkaConnectUrl) throws IOException {
        String connectorsUrl = combineUrls(kafkaConnectUrl, CONNECTORS_ENDPOINT);
        if (logger.isTraceEnabled()) {
            logger.trace("get kafka connectors by url: {}", connectorsUrl);
        }

        Request request = new Request.Builder().url(connectorsUrl).build();
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
        String connectorConfigUrl = combineUrls(kafkaConnectUrl, CONNECTORS_ENDPOINT, connector);
        if (logger.isTraceEnabled()) {
            logger.trace("get kafka connector config by url: {}", connectorConfigUrl);
        }

        Request request = new Request.Builder().url(connectorConfigUrl).build();
        try (Response response = client.newCall(request).execute()) {
            if (response.isSuccessful()) {
                JsonNode connectorCfg = JsonUtils.getJsonNode(Objects.requireNonNull(response.body()).string());
                return JsonUtils.jsonToMap(connectorCfg.get("config"));
            }
            throw new IOException("Unexpected code " + response);
        }
    }

    public static Schema getAvroSchema(String schemaRegistryUrl, String subjectName) throws IOException {
        String schemeUrl = combineUrls(schemaRegistryUrl, "subjects",
                subjectName, "versions", "latest");
        if (logger.isTraceEnabled()) {
            logger.trace("get avro schema by url: {}", schemeUrl);
        }

        Request request = new Request.Builder().url(schemeUrl).build();
        try (Response response = client.newCall(request).execute()) {
            if (response.isSuccessful()) {
                Schema.Parser parser = new Schema.Parser();
                return parser.parse(Objects.requireNonNull(response.body()).string());
            }
            throw new IOException("Unexpected code " + response);
        }
    }

    public static final String DEBEZIUM_VERSION = "1.3.1.Final";
}
