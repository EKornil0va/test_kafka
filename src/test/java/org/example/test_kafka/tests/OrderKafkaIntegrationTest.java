package org.example.test_kafka.tests;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.example.test_kafka.dto.OrderEvent;
import org.example.test_kafka.dto.OrderResult;
import org.example.test_kafka.producer.OrderProducer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@SpringBootTest
@ExtendWith(SpringExtension.class)
@Import(OrderKafkaIntegrationTest.KafkaTestConfiguration.class)
@Execution(ExecutionMode.CONCURRENT)
class OrderKafkaIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(OrderKafkaIntegrationTest.class);
    private static final String RESULT_TOPIC = "orders.processed";

    @Autowired
    private OrderProducer orderProducer;

    @Autowired
    private KafkaContainer kafka;

    @BeforeAll
     static void waitForOrderProcessor(@Autowired KafkaListenerEndpointRegistry registry) {
        var listener = registry.getListenerContainer("orderProcessor");
        assertNotNull(listener, "Kafka listener orderProcessor не найден");
        ContainerTestUtils.waitForAssignment(listener, 1);
    }

    static Stream<Arguments> orderTestData() {
        return Stream.of(
                Arguments.of("Положительная сумма", new BigDecimal("150.50"),
                        "ACCEPTED", "Order validated successfully"),
                Arguments.of("Отрицательная сумма", new BigDecimal("-50.00"),
                        "REJECTED", "Amount must be greater than 0"),
                Arguments.of("Ноль с дробной частью", new BigDecimal("0.00"),
                        "REJECTED", "Amount must be greater than 0")
        );
    }

    @ParameterizedTest(name = "{0}: amount={1}, ожидается {2}")
    @MethodSource("orderTestData")
    void processesOrder(String scenario, BigDecimal amount, String expectedStatus, String expectedReason)
            throws Exception {
        String orderId = "ORD-" + UUID.randomUUID();
        log.info("START | {} | orderId={} | amount={} | ожидается {}", scenario, orderId, amount, expectedStatus);

        OrderResult result;
        try (Consumer<String, OrderResult> consumer = newResultConsumer(orderId)) {
            consumer.subscribe(List.of(RESULT_TOPIC));
            orderProducer.sendOrder(new OrderEvent(orderId, amount)).get(15, TimeUnit.SECONDS);

            ConsumerRecord<String, OrderResult> record = await("Результат заказа " + orderId)
                    .dontCatchUncaughtExceptions()
                    .pollInSameThread()
                    .pollInterval(Duration.ofMillis(100))
                    .atMost(Duration.ofSeconds(20))
                    .until(() -> pollForOrder(consumer, orderId), Objects::nonNull);

            result = record.value();
            assertNotNull(result, "Сообщение должно содержать OrderResult");
            assertAll(
                    () -> assertEquals(orderId, record.key(), "Ключ Kafka"),
                    () -> assertEquals(orderId, result.orderId(), "Идентификатор в DTO"),
                    () -> assertEquals(expectedStatus, result.status(), "Статус"),
                    () -> assertEquals(expectedReason, result.reason(), "Причина")
            );
        } catch (Exception | AssertionError failure) {
            log.error("FAIL | {} | orderId={} | ожидался статус {}", scenario, orderId, expectedStatus, failure);
            throw failure;
        }
        log.info("PASS | {} | orderId={} | {}", scenario, orderId, result);
    }

    private Consumer<String, OrderResult> newResultConsumer(String orderId) {
        Map<String, Object> properties = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "order-result-test-" + orderId,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class,
                JsonDeserializer.VALUE_DEFAULT_TYPE, OrderResult.class.getName(),
                JsonDeserializer.TRUSTED_PACKAGES, "org.example.test_kafka.dto"
        );
        return new DefaultKafkaConsumerFactory<String, OrderResult>(properties).createConsumer();
    }

    private ConsumerRecord<String, OrderResult> pollForOrder(Consumer<String, OrderResult> consumer, String orderId) {
        for (ConsumerRecord<String, OrderResult> record : consumer.poll(Duration.ofMillis(300))) {
            if (orderId.equals(record.key())
                    || (record.value() != null && orderId.equals(record.value().orderId()))) {
                return record;
            }
        }
        return null;
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class KafkaTestConfiguration {

        @Bean
        @ServiceConnection
        KafkaContainer kafkaContainer() {
            return new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.5.0")).withKraft();
        }
    }
}
