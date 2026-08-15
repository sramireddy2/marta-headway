package dev.headway.ingest.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import dev.headway.common.Json;
import dev.headway.common.VehiclePosition;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.internals.BuiltInPartitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Tests the publisher against Kafka's own {@code MockProducer} — no broker, no Docker, no
 * Testcontainers. It records what would have been sent, which is exactly what we want to assert.
 */
class KafkaPositionPublisherTest {

    private static final Instant T0 = Instant.parse("2026-08-14T21:00:00Z");
    private static final String TOPIC = "vehicle-positions";
    private static final int PARTITIONS = KafkaPositionPublisher.DEFAULT_PARTITIONS;

    /**
     * Kafka 4's {@code MockProducer} requires an explicit partitioner. This one defers to
     * {@link BuiltInPartitioner}, the same class a real producer uses for a keyed record.
     */
    private static final class RouteKeyPartitioner implements Partitioner {
        @Override
        public int partition(String topic, Object key, byte[] keyBytes,
                             Object value, byte[] valueBytes, Cluster cluster) {
            return BuiltInPartitioner.partitionForKey(keyBytes, PARTITIONS);
        }

        @Override
        public void close() {}

        @Override
        public void configure(Map<String, ?> configs) {}
    }

    private static VehiclePosition ping(String vehicleId, String routeId) {
        return new VehiclePosition(vehicleId, routeId, "trip-1", 0,
                33.75, -84.39, null, null, T0);
    }

    private static MockProducer<String, VehiclePosition> mockProducer() {
        return new MockProducer<>(true, new RouteKeyPartitioner(),
                new StringSerializer(), new VehiclePositionSerializer());
    }

    @Test
    @DisplayName("every position in the batch is published to the topic")
    void publishesWholeBatch() {
        MockProducer<String, VehiclePosition> mock = mockProducer();
        try (KafkaPositionPublisher publisher = new KafkaPositionPublisher(mock, TOPIC)) {
            publisher.acceptAll(List.of(ping("bus-1", "15"), ping("bus-2", "15"), ping("bus-3", "110")));

            assertThat(mock.history()).hasSize(3);
            assertThat(mock.history()).allSatisfy(r -> assertThat(r.topic()).isEqualTo(TOPIC));
            assertThat(publisher.sentTotal()).isEqualTo(3);
            assertThat(publisher.failedTotal()).isZero();
        }
    }

    @Test
    @DisplayName("the record key is the route id, not the vehicle id")
    void keysByRoute() {
        MockProducer<String, VehiclePosition> mock = mockProducer();
        try (KafkaPositionPublisher publisher = new KafkaPositionPublisher(mock, TOPIC)) {
            publisher.acceptAll(List.of(ping("bus-1", "15"), ping("bus-2", "110")));

            assertThat(mock.history()).extracting(ProducerRecord::key)
                    .containsExactly("15", "110");
        }
    }

    @Test
    @DisplayName("the value on the wire is the agreed JSON, readable by any consumer")
    void serialisesValueAsJson() throws IOException {
        MockProducer<String, VehiclePosition> mock = mockProducer();
        VehiclePosition original = ping("bus-1", "15");

        try (KafkaPositionPublisher publisher = new KafkaPositionPublisher(mock, TOPIC)) {
            publisher.accept(original);
        }

        // MockProducer stores the deserialised object, so re-serialise to inspect the bytes a
        // real broker would receive.
        byte[] wire = new VehiclePositionSerializer().serialize(TOPIC, mock.history().getFirst().value());
        assertThat(Json.fromBytes(wire, VehiclePosition.class)).isEqualTo(original);
        assertThat(new String(wire)).contains("\"routeId\":\"15\"");
    }

    /**
     * The claim the whole design rests on, checked against Kafka's real partitioning function
     * rather than taken on faith.
     *
     * <p>This calls {@link BuiltInPartitioner#partitionForKey}, which is Kafka's own production
     * code for placing a keyed record — not a reimplementation of it. If Kafka ever changed how
     * keys map to partitions, this test would follow.
     */
    @Test
    @DisplayName("all records for one route hash to a single partition")
    void oneRouteLandsOnOnePartition() {
        Set<Integer> partitionsForRoute15 = List.of("bus-1", "bus-2", "bus-3", "bus-4").stream()
                .map(v -> ping(v, "15"))
                .map(vp -> partitionFor(vp.routeId()))
                .collect(Collectors.toSet());

        assertThat(partitionsForRoute15)
                .as("route 15 must never be split across partitions")
                .hasSize(1);
    }

    @Test
    @DisplayName("different routes do spread across partitions, so the topic still parallelises")
    void routesSpreadAcrossPartitions() {
        List<String> routes = List.of("1", "2", "3", "5", "15", "39", "110", "121", "186", "816");

        Set<Integer> used = routes.stream().map(KafkaPositionPublisherTest::partitionFor)
                .collect(Collectors.toSet());

        assertThat(used).as("keying by route should not funnel everything into one partition")
                .hasSizeGreaterThan(1);
    }

    private static int partitionFor(String key) {
        return BuiltInPartitioner.partitionForKey(key.getBytes(StandardCharsets.UTF_8), PARTITIONS);
    }

    @Test
    @DisplayName("an empty batch is a no-op, not an error")
    void handlesEmptyBatch() {
        MockProducer<String, VehiclePosition> mock = mockProducer();
        try (KafkaPositionPublisher publisher = new KafkaPositionPublisher(mock, TOPIC)) {
            publisher.acceptAll(List.of());

            assertThat(mock.history()).isEmpty();
            assertThat(publisher.sentTotal()).isZero();
        }
    }

    /**
     * The Ctrl+C path, exactly.
     *
     * <p>The shutdown hook closes the publisher, then {@code main} wakes from its latch and
     * try-with-resources closes it a second time. Without a guard the second call reaches
     * {@code producer.flush()} on a closed producer, which throws — printing a stack trace in the
     * middle of an otherwise clean shutdown.
     */
    @Test
    @DisplayName("close() twice is safe, because the shutdown hook and main both call it")
    void closeIsIdempotent() {
        MockProducer<String, VehiclePosition> mock = mockProducer();
        KafkaPositionPublisher publisher = new KafkaPositionPublisher(mock, TOPIC);
        publisher.accept(ping("bus-1", "15"));

        publisher.close();
        assertThat(publisher.isClosed()).isTrue();

        // Must not throw.
        publisher.close();
        publisher.close();

        assertThat(mock.history()).hasSize(1);
    }

    @Test
    @DisplayName("closing flushes so nothing is left in the linger buffer")
    void closeFlushes() {
        MockProducer<String, VehiclePosition> mock = mockProducer();
        KafkaPositionPublisher publisher = new KafkaPositionPublisher(mock, TOPIC);
        publisher.accept(ping("bus-1", "15"));

        publisher.close();

        assertThat(mock.closed()).isTrue();
        assertThat(mock.history()).hasSize(1);
    }
}
