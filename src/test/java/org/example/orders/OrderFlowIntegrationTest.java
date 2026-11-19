package org.example.orders;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.jdbc.JdbcTestUtils;
import org.testcontainers.kafka.KafkaContainer;

@SpringBootTest(properties = "spring.datasource.url=jdbc:h2:mem:flowtests;DB_CLOSE_DELAY=-1")
public class OrderFlowIntegrationTest {

    static final KafkaContainer kafka = new KafkaContainer("apache/kafka:4.0.0");

    static {
        kafka.start();
    }

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    @TestConfiguration
    static class Topics {

        @Bean
        NewTopic orderEvents() {
            return TopicBuilder.name("order-events").partitions(3).replicas(1).build();
        }

        @Bean
        NewTopic orderEventsDlt() {
            return TopicBuilder.name("order-events-dlt").partitions(3).replicas(1).build();
        }
    }

    @Autowired
    OrderService orderService;

    @Autowired
    JdbcTemplate jdbc;

    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;

    @BeforeEach
    void clean() {
        JdbcTestUtils.deleteFromTables(jdbc, "shipments", "processed_events", "outbox", "orders");
    }

    @Test
    void order_is_delivered_through_kafka_and_shipped() {
        String orderId = orderService.createOrder(new CreateOrderRequest("Ivan", 100));

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            assertThat(JdbcTestUtils.countRowsInTableWhere(jdbc, "outbox", "sent_at IS NOT NULL")).isEqualTo(1);
        });
        await().atMost(Duration.ofSeconds(20)).until(() -> shipmentsOf(orderId) == 1);
    }

    @Test
    void duplicate_event_does_not_create_second_shipment() throws Exception {
        String orderId = orderService.createOrder(new CreateOrderRequest("Valera", 50));
        await().atMost(Duration.ofSeconds(20)).until(() -> shipmentsOf(orderId) == 1);

        String payload = jdbc.queryForObject("SELECT payload FROM outbox", String.class);
        kafkaTemplate.send("order-events", orderId, payload).get();

        await().during(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(shipmentsOf(orderId)).isEqualTo(1));
    }

    @Test
    void poison_pill_goes_to_dlt_and_next_order_is_still_processed() throws Exception {
        String garbage = "это не json " + UUID.randomUUID();
        kafkaTemplate.send("order-events", garbage).get();

        String orderId = orderService.createOrder(new CreateOrderRequest("Anna", 300));

        ConsumerRecord<String, String> dead = awaitInDlt(garbage);
        Header originalTopic = dead.headers().lastHeader("kafka_dlt-original-topic");
        assertThat(originalTopic).isNotNull();
        assertThat(new String(originalTopic.value())).isEqualTo("order-events");

        await().atMost(Duration.ofSeconds(20)).until(() -> shipmentsOf(orderId) == 1);
    }

    private int shipmentsOf(String orderId) {
        return JdbcTestUtils.countRowsInTableWhere(jdbc, "shipments", "order_id = '" + orderId + "'");
    }

    private ConsumerRecord<String, String> awaitInDlt(String expectedValue) {
        Map<String, Object> props = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "dlt-test-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)){
            consumer.subscribe(List.of("order-events-dlt"));
            AtomicReference<ConsumerRecord<String, String>> found = new AtomicReference<>();

            await().atMost(Duration.ofSeconds(30)).pollInSameThread().until(() -> {
                for (ConsumerRecord<String, String> r : consumer.poll(Duration.ofMillis(500))) {
                    if (expectedValue.equals(r.value())) {
                        found.set(r);
                    }
                }
                return found.get() != null;
            });
            return found.get();
        }
    }
}
