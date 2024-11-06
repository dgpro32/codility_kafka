package com.codility.kafka;

import com.codility.event.Message;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class MessageProducer {
    @Value("${kafka.topics.one}")
    private final String topicName;
    private final KafkaTemplate<String, Message> kafkaTemplate;


    MessageProducer(@Value("${kafka.topics.one}") String topicName, KafkaTemplate<String, Message> kafkaTemplate) {
        this.topicName = topicName;
        this.kafkaTemplate = kafkaTemplate;
    }

    public void send(Message message) {
        this.kafkaTemplate.send(this.topicName, message);

    }
}
