package com.codility.kafka;

import com.codility.event.Message;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.apache.kafka.common.serialization.Deserializer;


@Slf4j
public class MessageDeserializer implements Deserializer<Message> {

    private final JsonMapper jsonMapper;

    public MessageDeserializer() {
        jsonMapper = JsonMapper.builder()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true)
                .build();
    }

    @Override
    public Message deserialize(String s, byte[] bytes) {
        try {
            return jsonMapper.readValue(bytes, Message.class);
        } catch (Exception e) {

            return null;
        }
    }
