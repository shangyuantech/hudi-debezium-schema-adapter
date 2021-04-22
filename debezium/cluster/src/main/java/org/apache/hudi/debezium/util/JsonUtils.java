package org.apache.hudi.debezium.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class JsonUtils {

    private static final ObjectMapper objectMapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    public static <T> List<T> jsonToList(String json) throws JsonProcessingException {
        return objectMapper.readValue(json, new TypeReference<List<T>>(){});
    }

    public static <T, R> Map<T, R> jsonToMap(JsonNode jsonNode) throws IOException {
        return objectMapper.readValue(objectMapper.treeAsTokens(jsonNode), new TypeReference<Map<T, R>>(){});
    }

    public static JsonNode getJsonNode(String json) throws JsonProcessingException {
        return objectMapper.readTree(json);
    }

    public static <T> String writeValueAsString(T object) throws JsonProcessingException {
        return objectMapper.writeValueAsString(object);
    }

    public static <T> T readValue(String json, Class<T> clazz) throws JsonProcessingException {
        return objectMapper.readValue(json, clazz);
    }

    public static <T> List<T> transformList(List<Map<String, Object>> data, Class<? extends T> type) {
        if (data == null || data.size() == 0) {
            return Collections.emptyList();
        }
        // https://stackoverflow.com/a/66339984/10660792
        return data.stream().map(response -> objectMapper.convertValue(response, type))
                .collect(Collectors.toList());
    }
}
