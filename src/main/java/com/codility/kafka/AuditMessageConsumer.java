package com.codility.kafka;

import com.codility.audit.AuditLog;
import com.codility.audit.AuditService;
import com.codility.event.Message;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class AuditMessageConsumer {
    private final AuditService auditService;
    @Value("${kafka.topics.one}")
    private String topicName;
    @Value("${kafka.groups.two}")
    private String groupId;

    AuditMessageConsumer(AuditService auditService) {
        this.auditService = auditService;
    }


    @KafkaListener(
            topics = topicName,
            containerFactory = "listenerContainerFactory",
            groupId = groupId
    )
    public void receive(Message message) {

        String author = message.author();
        String content = message.title();
        AuditLog auditLog = new AuditLog(author, content);
        AuditService.info(auditLog);

    }
}
