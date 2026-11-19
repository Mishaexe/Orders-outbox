package org.example.orders;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

@Component
public class OrderEventsListener {

    private final ShipmentService shipmentService;
    private final JsonMapper json;

    public OrderEventsListener(ShipmentService shipmentService, JsonMapper json) {
        this.shipmentService = shipmentService;
        this.json = json;
    }

    @KafkaListener(topics = "order-events")
    public void onOrderCreated(String payload) {
        OrderCreatedEvent event;
        try {
            event = json.readValue(payload, OrderCreatedEvent.class);
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("Не удалось разобрать JSON события", e);
        }
        shipmentService.handle(event);
    }
}
