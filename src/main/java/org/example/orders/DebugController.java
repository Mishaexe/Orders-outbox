package org.example.orders;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/debug")
public class DebugController {

    private final JdbcTemplate jdbc;

    public DebugController(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @GetMapping("/orders")
    public List<Map<String, Object>> orders() {
        return jdbc.queryForList("SELECT id, customer, amount FROM orders ORDER BY created_at");
    }

    @GetMapping("/outbox")
    public List<Map<String, Object>> outbox() {
        return jdbc.queryForList("SELECT id, event_id, aggregate_id, event_type, sent_at FROM outbox ORDER BY id");
    }

    @GetMapping("/shipments")
    public List<Map<String, Object>> shipments() {
        return jdbc.queryForList("SELECT id, order_id, amount FROM shipments ORDER BY id");
    }

    @GetMapping("/processed")
    public List<Map<String, Object>> processed() {
        return jdbc.queryForList("SELECT event_id, processed_at FROM processed_events ORDER BY processed_at");
    }
}
