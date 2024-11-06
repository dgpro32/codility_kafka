package com.codility.kafka;

import com.codility.clock.Clock;
import com.codility.event.Message;
import com.codility.repositories.MessageEntity;
import com.codility.repositories.PersistenceRepository;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class PersistenceMessageConsumer {
    private final PersistenceRepository persistenceRepository;
    private final Clock clock;
    @Value("${kafka.topics.one}")
    private String topicName;
    @Value("${kafka.groups.one}")
    private String groupId;


    PersistenceMessageConsumer(PersistenceRepository persistenceRepository, Clock clock) {
        this.persistenceRepository = persistenceRepository;
        this.clock = clock;
    }

    @KafkaListener(
            topics = topicName,
            containerFactory = "listenerContainerFactory",
            groupId = groupId
    )
    public void receive(Message message) {

        LocalDate receiveDate = clock.now();
        String author = message.author();
        String title = message.title();
        String payload = message.payload();
        MessageEntity messageEntity = new MesaageEntity(receiveDate, author, title, payload);
        persistenceRepository.save(messageEntity);

    }
}
