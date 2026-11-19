package org.example.orders;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.jdbc.JdbcTestUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:dbtests;DB_CLOSE_DELAY=-1",
        "spring.kafka.listener.auto-startup=false",
        "outbox.publisher.enabled=false"
})
public class ShipmentServiceTest {

    @Autowired
    ShipmentService shipmentService;

    @Autowired
    JdbcTemplate jdbc;

    @BeforeEach
    void clean() {
        JdbcTestUtils.deleteFromTables(jdbc, "shipments", "processed_events", "outbox", "orders");
    }

    @Test
    void same_event_twice_creates_one_shipment() {
        var event = new OrderCreatedEvent("evt-1", "order-1", "Ivan", 100);

        shipmentService.handle(event);
        shipmentService.handle(event);

        assertThat(JdbcTestUtils.countRowsInTable(jdbc, "shipments")).isEqualTo(1);
        assertThat(JdbcTestUtils.countRowsInTable(jdbc, "processed_events")).isEqualTo(1);
    }

    @Test
    void invalid_amount_leaves_no_trace_in_journal() {
        var event = new OrderCreatedEvent("evt-2", "order-2", "Ivan", -5);

        assertThatThrownBy(() -> shipmentService.handle(event))
                .isInstanceOf(IllegalArgumentException.class);

        assertThat(JdbcTestUtils.countRowsInTable(jdbc, "processed_events")).isZero();
        assertThat(JdbcTestUtils.countRowsInTable(jdbc, "shipments")).isZero();
    }

    @Test
    void temporary_failure_leaves_no_trace_in_journal() {
        var event = new OrderCreatedEvent("evt-3", "order-3", "broken", 100);

        assertThatThrownBy(() -> shipmentService.handle(event))
                .isInstanceOf(IllegalStateException.class);

        assertThat(JdbcTestUtils.countRowsInTable(jdbc, "processed_events")).isZero();
        assertThat(JdbcTestUtils.countRowsInTable(jdbc, "shipments")).isZero();
    }
}
