package org.example.test_kafka.producer;

import java.util.concurrent.CompletableFuture;
import org.example.test_kafka.dto.OrderEvent;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

@Service
public class OrderProducer {
    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;

    public OrderProducer(KafkaTemplate<String, OrderEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public CompletableFuture<SendResult<String, OrderEvent>> sendOrder(OrderEvent event) {
        return kafkaTemplate.send("orders.created", event.orderId(), event);
    }
}
