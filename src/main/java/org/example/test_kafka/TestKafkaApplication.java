package org.example.test_kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;

@SpringBootApplication
public class TestKafkaApplication {

    public static void main(String[] args) {
        SpringApplication.run(TestKafkaApplication.class, args);
    }

    @Bean
    NewTopic ordersCreated() {
        return TopicBuilder.name("orders.created").partitions(1).replicas(1).build();
    }

    @Bean
    NewTopic ordersProcessed() {
        return TopicBuilder.name("orders.processed").partitions(1).replicas(1).build();
    }
}
