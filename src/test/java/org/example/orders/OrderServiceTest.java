package org.example.orders;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.jdbc.JdbcTestUtils;

@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:dbtests;DB_CLOSE_DELAY=-1",
        "spring.kafka.listener.auto-startup=false",
        "outbox.publisher.enabled=false"
})
public class OrderServiceTest {

    @Autowired
    OrderService orderService;

    @Autowired
    JdbcTemplate jdbc;

    @BeforeEach
    void clean() {
        JdbcTestUtils.deleteFromTables(jdbc, "shipments", "processed_events", "outbox", "orders");
    }

    @Test
    void order_and_outbox_record_are_saved_together() {
        orderService.createOrder(new CreateOrderRequest("Ivan", 100));

        assertThat(JdbcTestUtils.countRowsInTable(jdbc, "orders")).isEqualTo(1);
        assertThat(JdbcTestUtils.countRowsInTable(jdbc, "outbox")).isEqualTo(1);
        assertThat(JdbcTestUtils.countRowsInTableWhere(jdbc, "outbox", "sent_at IS NULL")).isEqualTo(1);
    }

    @Test
    void failure_after_outbox_insert_rolls_back_both() {
        assertThatThrownBy(() -> orderService.createOrder(new CreateOrderRequest("fail", 50)))
                .isInstanceOf(IllegalStateException.class);

        assertThat(JdbcTestUtils.countRowsInTable(jdbc, "orders")).isZero();
        assertThat(JdbcTestUtils.countRowsInTable(jdbc, "outbox")).isZero();
    }
}
