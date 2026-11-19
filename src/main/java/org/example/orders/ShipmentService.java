package org.example.orders;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;
import java.time.Instant;

@Service
public class ShipmentService {

    private static final Logger log = LoggerFactory.getLogger(ShipmentService.class);

    private final JdbcTemplate jdbc;
    private final boolean crashMidHandle;

    public ShipmentService(JdbcTemplate jdbc, @Value("${chaos.crash-mid-handle:false}") boolean crashMidHandle) {
        this.jdbc = jdbc;
        this.crashMidHandle = crashMidHandle;
    }

    @Transactional
    public void handle(OrderCreatedEvent event) {
        log.info("Обрабатываю событие {} (заказ {})", event.eventId(), event.orderId());
        try {
            jdbc.update("INSERT INTO processed_events (event_id, processed_at) VALUES (?, ?)",
                    event.eventId(), Timestamp.from(Instant.now()));
        } catch (DuplicateKeyException e) {
            log.info("Дубль события {}, пропускаю", event.eventId());
            return;
        }

        if (event.amount() <= 0) {
            throw new IllegalArgumentException("Некорректная сумма: " + event.amount());
        }

        if ("broken".equals(event.customer())) {
            throw new IllegalStateException("Склад временно недоступен");
        }

        if (crashMidHandle) {
            log.warn("ХАОС: процесс падает посреди обработки (журнал записан, отгрузки еще нет)");
            Runtime.getRuntime().halt(1);
        }

        jdbc.update("INSERT INTO shipments (order_id, amount) VALUES (?, ?)",
                event.orderId(), event.amount());
        log.info("Оформлена отгрузка заказа {} (событие {})", event.orderId(), event.eventId());
    }
}
