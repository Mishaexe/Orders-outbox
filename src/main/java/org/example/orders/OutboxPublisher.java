package org.example.orders;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Component
@ConditionalOnProperty(name = "out.box.publisher.enabled", havingValue = "true", matchIfMissing = true)
public class OutboxPublisher {

    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);

    private record OutboxRow(long id, String aggregateId, String payload) {}

    private final JdbcTemplate jdbc;
    private final KafkaTemplate<String, String> kafka;
    private final boolean crashAfterSend;

    public OutboxPublisher(JdbcTemplate jdbc, KafkaTemplate<String, String> kafka, @Value("${chaos.crash-after-send:false}") boolean crashAfterSend) {
        this.jdbc = jdbc;
        this.kafka = kafka;
        this.crashAfterSend = crashAfterSend;
    }

    @Scheduled(fixedDelay = 1000)
    public void publish() {
        List<OutboxRow> rows = jdbc.query(
             "SELECT id, aggregate_id, payload FROM outbox WHERE sent_at IS NULL ORDER BY id LIMIT 50",
                (rs, i) -> new OutboxRow(rs.getLong("id"), rs.getString("aggregate_id"), rs.getString("payload")));

        for (OutboxRow row : rows) {
            try {
                kafka.send("order-events", row.aggregateId(), row.payload()).get(10, TimeUnit.SECONDS);

                if (crashAfterSend) {
                    log.warn("ХАОС: процесс падает после отправки в Kafka, до отметки sent_at");
                    Runtime.getRuntime().halt(1);
                }
                jdbc.update("UPDATE outbox set sent_at = ? where id = ?",
                        Timestamp.from(Instant.now()), row.id());
                log.info("Отправлено событие outbox id = {}", row.id());
            } catch (Exception e) {
                log.warn("Не удалось отправить outbox id = {}: {}", row.id(), e.toString());
                break;
            }
        }
    }
}
