package com.github.devwdougherty.demos.kafka.streams.wikimedia.processor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.*;

import java.io.IOException;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class WebsiteCountStreamBuilder {

    private static final String WEBSITE_COUNT_STORE = "website-count-store";
        private static final String WEBSITE_COUNT_TOPIC = "website.stats.website";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final KStream<String, String> inputStream;

    public WebsiteCountStreamBuilder(KStream<String, String> inputStream) {
        this.inputStream = inputStream;
    }

    public void setup() {

        final TimeWindows timeWindows = TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1L));

        this.inputStream
                .selectKey((k, changeJson) -> {

                    try {

                        final JsonNode jsonNode = OBJECT_MAPPER.readTree(changeJson);
                        return jsonNode.get("server_name").asText();
                    } catch (IOException ioException) {
                        return "parser-error";
                    }
                })
                .groupByKey()
                .windowedBy(timeWindows)
                .count(Materialized.as(WEBSITE_COUNT_STORE))
                .toStream()
                .mapValues((key, value) -> {

                    final Map<String, Object> kvMap = Stream.of(
                            new AbstractMap.SimpleEntry<>(String.valueOf(key), value)).collect(
                            Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

                    try {
                        return OBJECT_MAPPER.writeValueAsString(kvMap);
                    } catch(JsonProcessingException jsonProcessingException) {
                        return null;
                    }
                })
                .to(WEBSITE_COUNT_TOPIC, Produced.with(
                        WindowedSerdes.timeWindowedSerdeFrom(String.class, timeWindows.size()),
                        Serdes.String()
                ));
    }
}
