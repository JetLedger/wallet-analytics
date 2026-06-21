package com.jetledger.analytics.infrastructure.consumer;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.core.JacksonException;
import java.util.Optional;

public record CloudEvent(
    String specversion,
    String type,
    String source,
    String id,
    String time,
    String datacontenttype,
    JsonNode data
) {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static Optional<CloudEvent> fromJson(String json) {
        try {
            JsonNode root = MAPPER.readTree(json);
            return Optional.of(new CloudEvent(
                root.get("specversion").asText(),
                root.get("type").asText(),
                root.get("source").asText(),
                root.get("id").asText(),
                root.get("time").asText(),
                root.get("datacontenttype").asText(),
                root.get("data")
            ));
        } catch (JacksonException e) {
            return Optional.empty();
        }
    }
}
