package com.codility.kafka;

import com.codility.event.Message;
import com.codility.processing.MessageProcessor;
import com.codility.repositories.UserEntity;
import com.codility.repositories.UserRepository;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class MessageConsumer {
    private final UserRepository userRepository;
    private final MessageProcessor messageProcessor;
    @Value("${kafka.topics.one}")
    private String topicName;
    @Value("${kafka.groups.three}")
    private String groupId;

    public MessageConsumer(UserRepository userRepository, MessageProcessor messageProcessor) {
        this.userRepository = userRepository;
        this.messageProcessor = messageProcessor;
    }

    @KafkaListener(
            topics = topicName,
            containerFactory = "listenerContainerFactory",
            groupId = groupId
    )
    public void receive(Message message) {
        String login = message.author();
        UserEntity userEntity = userRepository.find(login);
        messageProcessor.process(userEntity, message);

    }
}

