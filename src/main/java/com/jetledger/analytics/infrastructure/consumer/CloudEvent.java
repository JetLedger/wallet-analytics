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
                root.path("specversion").asText(),
                root.path("type").asText(),
                root.path("source").asText(),
                root.path("id").asText(),
                root.path("time").asText(),
                root.path("datacontenttype").asText(),
                root.path("data")
            ));
        } catch (JacksonException e) {
            return Optional.empty();
        }
    }
}
