package org.example.orders;

public record OrderCreatedEvent(String eventId, String orderId, String customer, int amount) {
}
