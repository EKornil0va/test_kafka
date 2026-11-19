package org.example.test_kafka.dto;

import java.math.BigDecimal;

public record OrderEvent(String orderId, BigDecimal amount) {}
