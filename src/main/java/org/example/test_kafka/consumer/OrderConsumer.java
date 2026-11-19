package org.example.test_kafka.consumer;

import java.math.BigDecimal;
import org.example.test_kafka.dto.OrderEvent;
import org.example.test_kafka.dto.OrderResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class OrderConsumer {
    private static final Logger log = LoggerFactory.getLogger(OrderConsumer.class);
    private final KafkaTemplate<String, OrderResult> kafkaTemplate;

    public OrderConsumer(KafkaTemplate<String, OrderResult> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @KafkaListener(id = "orderProcessor", topics = "orders.created", groupId = "order-processing-group")
    public void processOrder(OrderEvent event) {
        log.info("Получено событие {}", event);

        OrderResult result;
        if (event.amount().compareTo(BigDecimal.ZERO) > 0) {
            result = new OrderResult(event.orderId(), "ACCEPTED", "Order validated successfully");
        } else {
            result = new OrderResult(event.orderId(), "REJECTED", "Amount must be greater than 0");
        }

        log.info("Отправка результата {}", result);
        kafkaTemplate.send("orders.processed", result.orderId(), result);
    }
}
