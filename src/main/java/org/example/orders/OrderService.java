package org.example.orders;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.json.JsonMapper;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.UUID;

@Service
public class OrderService {

    private final JdbcTemplate jdbc;
    private final JsonMapper json;

    public OrderService(JdbcTemplate jdbc, JsonMapper json) {
        this.jdbc = jdbc;
        this.json = json;
    }

    @Transactional
    public String createOrder(CreateOrderRequest req) {
        String orderId = UUID.randomUUID().toString();
        String eventId = UUID.randomUUID().toString();
        Timestamp now = Timestamp.from(Instant.now());

        jdbc.update("INSERT INTO orders (id, customer, amount, created_at) VALUES (?, ?, ?, ?)",
                orderId, req.customer(), req.amount(), now);

        OrderCreatedEvent event = new OrderCreatedEvent(eventId, orderId, req.customer(), req.amount());
        jdbc.update("INSERT INTO outbox (event_id, aggregate_id, event_type, payload, created_at) " + "VALUES (?, ?, ?, ?, ? )",
                eventId, orderId, "OrderCreated", json.writeValueAsString(event), now);

        if ("fail".equals(req.customer())) {
            throw new IllegalStateException("Сбой после записи в outbox");
        }
        return orderId;
    }
}
