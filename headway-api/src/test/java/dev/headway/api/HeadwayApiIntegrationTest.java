package dev.headway.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.JsonNode;
import dev.headway.api.ingest.RecordStreamFactory;
import dev.headway.api.model.AlertEpisode;
import dev.headway.api.model.RouteHeadway;
import dev.headway.api.model.ServiceStatus;
import dev.headway.common.Json;
import dev.headway.common.VehiclePosition;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.http.ResponseEntity;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;
import org.springframework.web.socket.handler.TextWebSocketHandler;

/**
 * The whole application, wired exactly as it runs, with fakes only where it would touch a broker.
 *
 * <p>Unit tests cover each piece; this covers the parts that only exist once they are assembled —
 * that the consumer threads actually start, that {@code SmartLifecycle} runs them at the right
 * time, that a record read from a topic reaches an HTTP response, and that a browser connecting to
 * the socket is greeted with the current state rather than silence.
 *
 * <p>Each test uses its own route groups. The context is shared across methods, so asserting on
 * global counts would make the tests order-dependent — and a suite that passes only in one order
 * is a suite that will fail for someone else.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "headway.poll-timeout=50ms",
        "headway.broadcast-interval-millis=200",
        // Long enough that housekeeping never fires mid-test; the sweep has its own unit tests.
        "headway.sweep-interval-millis=600000",
        "logging.level.dev.headway=WARN"
})
@Timeout(60)
class HeadwayApiIntegrationTest {

    private static final Instant NOW = Instant.parse("2026-08-15T14:00:00Z");

    /**
     * Replaces the one bean that opens a socket to Kafka.
     *
     * <p>{@code @Primary} rather than excluding the real factory: the real bean is still defined,
     * still built, and still proves it can be constructed — it simply never wins injection. That
     * keeps the test honest about what the production context contains.
     */
    @TestConfiguration
    static class FakeStreams {

        final Map<String, QueueRecordStream> created = new ConcurrentHashMap<>();

        @Bean
        @Primary
        RecordStreamFactory fakeRecordStreamFactory() {
            return prefix -> created.computeIfAbsent(prefix, key -> new QueueRecordStream());
        }

        QueueRecordStream headways() {
            return await(created, "headway-api-headways");
        }

        QueueRecordStream vehicles() {
            return await(created, "headway-api-vehicles");
        }

        private static QueueRecordStream await(Map<String, QueueRecordStream> streams, String key) {
            org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(10))
                    .until(() -> streams.containsKey(key));
            return streams.get(key);
        }
    }

    @Autowired
    private FakeStreams streams;

    @Autowired
    private TestRestTemplate rest;

    @LocalServerPort
    private int port;

    private void publishHeadway(String group, int secondsIn, String status, double ratio) {
        streams.headways().publish(
                Payloads.headway(group, NOW.plusSeconds(secondsIn), status, ratio, 500.0));
    }

    @Test
    @DisplayName("a record on the topic becomes an HTTP response")
    void topicToRest() {
        publishHeadway("77:1", 60, "SEVERE_BUNCHING", 0.18);

        RouteHeadway headway = awaitRoute("77:1");
        assertThat(headway.headwayGroup()).isEqualTo("77:1");
        assertThat(headway.status()).isEqualTo("SEVERE_BUNCHING");
        assertThat(headway.scheduledHeadwaySeconds()).isEqualTo(1800);
        assertThat(headway.windowEnd()).isEqualTo(NOW.plusSeconds(60));

        assertThat(rest.getForEntity("/api/routes/does-not-exist", String.class).getStatusCode()
                .value()).isEqualTo(404);
    }

    /**
     * The point of the whole module. Six overlapping windows arrive on the topic; {@code
     * /api/alerts} shows one row.
     */
    @Test
    @DisplayName("six window records for one event surface as one alert")
    void duplicateWindowsBecomeOneAlert() {
        for (int i = 0; i < 6; i++) {
            publishHeadway("88:0", 60 + i * 30, "SEVERE_BUNCHING", 0.2);
        }

        await().atMost(Duration.ofSeconds(10)).until(() -> episodeFor("88:0") != null
                && episodeFor("88:0").windowsObserved() == 6);

        AlertEpisode episode = episodeFor("88:0");
        assertThat(episode.startedAt()).isEqualTo(NOW.plusSeconds(60));
        assertThat(episode.lastSeenAt()).isEqualTo(NOW.plusSeconds(210));
        assertThat(episode.endedAt()).isNull();

        // Assert against the raw JSON, not the deserialised record. durationSeconds is a computed
        // accessor, and a parsed AlertEpisode recomputes it from startedAt — so the record-based
        // assertion passes even when the field is absent from the wire, which is exactly the bug
        // that shipped first.
        assertThat(rest.getForObject("/api/alerts", String.class))
                .contains("\"durationSeconds\":150");

        // ...and recovery closes it, moving it to history.
        publishHeadway("88:0", 240, "ON_SCHEDULE", 1.0);
        await().atMost(Duration.ofSeconds(10)).until(() -> episodeFor("88:0") == null);

        assertThat(rest.getForObject("/api/alerts/history", AlertEpisode[].class))
                .anyMatch(closed -> closed.headwayGroup().equals("88:0")
                        && closed.endedAt() != null
                        && closed.windowsObserved() == 6);
    }

    @Test
    @DisplayName("vehicle positions are served for the map")
    void vehiclesReachTheMap() {
        streams.vehicles().publish(Payloads.vehicle("9001", "110", NOW));
        streams.vehicles().publish(Payloads.vehicle("9002", "110", NOW));
        streams.vehicles().publish(Payloads.vehicle("9003", "111", NOW));

        await().atMost(Duration.ofSeconds(10)).until(() ->
                rest.getForObject("/api/vehicles?routeId=110", VehiclePosition[].class).length == 2);

        VehiclePosition[] onRoute = rest.getForObject("/api/vehicles?routeId=110",
                VehiclePosition[].class);
        assertThat(onRoute).extracting(VehiclePosition::vehicleId)
                .containsExactlyInAnyOrder("9001", "9002");
        assertThat(onRoute[0].latitude()).isEqualTo(33.7490);
    }

    @Test
    @DisplayName("a browser connecting is sent the current state immediately")
    void webSocketGreetsWithASnapshot() throws Exception {
        publishHeadway("66:1", 60, "BUNCHING", 0.4);
        awaitRoute("66:1");

        BlockingQueue<String> frames = new LinkedBlockingQueue<>();
        WebSocketSession session = new StandardWebSocketClient()
                .execute(new TextWebSocketHandler() {
                    @Override
                    protected void handleTextMessage(WebSocketSession s, TextMessage message) {
                        frames.add(message.getPayload());
                    }
                }, "ws://localhost:" + port + "/ws/live")
                .get(10, TimeUnit.SECONDS);

        try {
            String frame = frames.poll(10, TimeUnit.SECONDS);
            assertThat(frame).as("the initial frame, not a scheduled tick").isNotNull();

            JsonNode snapshot = Json.mapper().readTree(frame);
            assertThat(snapshot.get("generatedAt").asText()).isNotBlank();
            assertThat(snapshot.get("sequence").asLong()).isPositive();
            assertThat(snapshot.get("routes").isArray()).isTrue();
            assertThat(snapshot.get("routes").findValuesAsText("headwayGroup"))
                    .contains("66:1");
            assertThat(snapshot.get("status").get("connectedClients").asInt()).isPositive();

            // And the tick keeps pushing while a client is attached.
            publishHeadway("66:1", 120, "SEVERE_BUNCHING", 0.15);
            await().atMost(Duration.ofSeconds(10)).until(() -> {
                String next = frames.poll(1, TimeUnit.SECONDS);
                return next != null && next.contains("SEVERE_BUNCHING");
            });
        } finally {
            session.close();
        }
    }

    @Test
    @DisplayName("status reports what the consumers are doing")
    void statusIsHonest() {
        publishHeadway("55:0", 60, "ON_SCHEDULE", 1.0);
        streams.headways().publish("definitely not json");

        await().atMost(Duration.ofSeconds(10))
                .until(() -> rest.getForObject("/api/status", ServiceStatus.class)
                        .malformedRecords() > 0);

        ServiceStatus status = rest.getForObject("/api/status", ServiceStatus.class);

        assertThat(status.consuming()).isTrue();
        assertThat(status.topics()).extracting(ServiceStatus.TopicStatus::name)
                .containsExactlyInAnyOrder("headways", "vehicles");
        assertThat(status.topics()).allMatch(ServiceStatus.TopicStatus::running);
        assertThat(status.routes()).isPositive();
        assertThat(status.headwayRecords()).isPositive();
        assertThat(status.newestWindowEnd()).isNotNull();
    }

    /**
     * The map is static files served off the classpath, so the failure mode is not a stack trace —
     * it is a 404 that nobody notices until they open a browser. Worth three assertions.
     */
    @Test
    @DisplayName("the live map is served at the root")
    void mapIsServed() {
        ResponseEntity<String> page = rest.getForEntity("/", String.class);

        assertThat(page.getStatusCode().is2xxSuccessful()).isTrue();
        assertThat(page.getBody()).contains("<title>Headway");

        for (String asset : new String[] {"/app.js", "/style.css"}) {
            assertThat(rest.getForEntity(asset, String.class).getStatusCode().is2xxSuccessful())
                    .as(asset).isTrue();
        }

        // The page and the server have to agree on the socket path; nothing else checks it.
        assertThat(rest.getForObject("/app.js", String.class)).contains("/ws/live");
    }

    @Test
    @DisplayName("both consumers subscribed to the configured topics")
    void wiring() {
        assertThat(streams.headways().subscribedTopic()).isEqualTo("route-headways");
        assertThat(streams.vehicles().subscribedTopic()).isEqualTo("vehicle-positions");
        assertThat(streams.headways()).isNotSameAs(streams.vehicles());
    }

    /**
     * Waits for a group to appear, and returns it.
     *
     * <p>Checking the status code rather than just a non-null body, because Spring's 404 page is
     * itself JSON — {@code {"timestamp":...,"status":404,...}} — and with unknown properties
     * ignored it deserialises quite happily into a {@code RouteHeadway} with every field null. A
     * "wait until the response is not null" loop is therefore satisfied instantly by a 404, which
     * is how the first version of this test came to assert against a record that had not arrived.
     */
    private RouteHeadway awaitRoute(String group) {
        await().atMost(Duration.ofSeconds(10)).until(() -> {
            ResponseEntity<RouteHeadway> response =
                    rest.getForEntity("/api/routes/" + group, RouteHeadway.class);
            return response.getStatusCode().is2xxSuccessful()
                    && response.getBody() != null
                    && group.equals(response.getBody().headwayGroup());
        });
        return rest.getForObject("/api/routes/" + group, RouteHeadway.class);
    }

    private AlertEpisode episodeFor(String group) {
        AlertEpisode[] open = rest.getForObject("/api/alerts", AlertEpisode[].class);
        if (open == null) {
            return null;
        }
        for (AlertEpisode episode : open) {
            if (episode.headwayGroup().equals(group)) {
                return episode;
            }
        }
        return null;
    }
}
