package com.example.kafkalab.consumer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class MessageConsumer {

    @KafkaListener(topics = "messages", groupId = "message-group")
    public void consume(String message) {
        log.info("Получено сообщение: {}", message);

        if ("FAIL".equals(message)) {
            log.error("Ошибка обработки сообщения: {}", message);
            throw new IllegalArgumentException("Ошибка обработки сообщения");
        }

        log.info("Сообщение успешно обработано: {}", message);
    }
}
