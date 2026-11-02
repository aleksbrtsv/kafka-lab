package com.example.kafkalab;

import com.example.kafkalab.producer.MessageProducer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.time.Duration;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

// Запускаем Spring-контекст приложения для тестирования
@SpringBootTest

// Поднимаем тестовую Kafka с двумя топиками
@EmbeddedKafka(
        topics = {"messages", "messages-dlt"},
        partitions = 1,
        bootstrapServersProperty = "spring.kafka.bootstrap-servers"
)
class KafkaDltIntegrationTest {

    // Получаем Producer из Spring-контейнера
    @Autowired
    private MessageProducer messageProducer;

    // Получаем доступ к тестовому Kafka Broker
    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;

    @Test
    void shouldSendFailedMessageToDlt() {

        // Настройки Consumer с отдельной группой
        Map<String, Object> props = KafkaTestUtils.consumerProps(
                embeddedKafka,
                "test-dlt-group",
                false
        );

        // Настраиваем преобразование ключа и сообщения из байтов в String
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        // Создаём фабрику Consumer с заданными настройками
        DefaultKafkaConsumerFactory<String, String> factory = new DefaultKafkaConsumerFactory<>(props);

        // Создаём Consumer и автоматически закрываем его после теста
        try (Consumer<String, String> consumer = factory.createConsumer()) {

            // Настраиваем чтение сообщений из DLQ
            embeddedKafka.consumeFromAnEmbeddedTopic(
                    consumer,
                    "messages-dlt"
            );

            // Отправляем сообщение, которое вызовет ошибку обработки
            messageProducer.sendMessage("FAIL");

            // Ожидаем сообщение из DLQ максимум 20 секунд
            ConsumerRecord<String, String> record =
                    KafkaTestUtils.getSingleRecord(
                            consumer,
                            "messages-dlt",
                            Duration.ofSeconds(20)
                    );

            // Проверяем, что в DLQ пришло именно наше сообщение
            assertEquals("FAIL", record.value());
        }
    }
}