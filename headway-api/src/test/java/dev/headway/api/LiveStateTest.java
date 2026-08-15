package dev.headway.api;

import static org.assertj.core.api.Assertions.assertThat;

import dev.headway.api.model.RouteHeadway;
import dev.headway.api.state.LiveHeadwayState;
import dev.headway.api.state.VehicleState;
import dev.headway.common.VehiclePosition;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Turning a log of measurements into "what is true right now". */
@Timeout(30)
class LiveStateTest {

    private static final Instant NOW = Instant.parse("2026-08-15T14:00:00Z");

    private static RouteHeadway at(String group, Instant windowEnd, String status, Double ratio) {
        return new RouteHeadway(windowEnd.minusSeconds(60), windowEnd, group,
                group.split(":")[0], "Route " + group, 0, 10_000.0, 3,
                500.0, 700.0, 900.0, 700.0, List.of(500.0, 900.0),
                List.of("a", "b", "c"), List.of(0.0, 500.0, 1400.0), List.of(),
                600, 3000.0, 5.0, 100.0, ratio, status, List.of("a", "b"));
    }

    @Nested
    @DisplayName("headways")
    class Headways {

        private final LiveHeadwayState state = new LiveHeadwayState();

        @Test
        @DisplayName("the newest window for a group replaces the previous one")
        void newestWins() {
            assertThat(state.accept(at("10:1", NOW, "ON_SCHEDULE", 1.0))).isTrue();
            assertThat(state.accept(at("10:1", NOW.plusSeconds(30), "BUNCHING", 0.4))).isTrue();

            assertThat(state.size()).isEqualTo(1);
            assertThat(state.group("10:1").orElseThrow().status()).isEqualTo("BUNCHING");
        }

        /**
         * Spark's Update output mode re-emits a window whenever late data refines it, so an older
         * window can arrive after a newer one. Letting it land would make the dashboard jump
         * backwards for no visible reason.
         */
        @Test
        @DisplayName("a late, older window is rejected rather than overwriting the present")
        void staleIsRejected() {
            state.accept(at("10:1", NOW.plusSeconds(60), "BUNCHING", 0.4));

            assertThat(state.accept(at("10:1", NOW, "ON_SCHEDULE", 1.0))).isFalse();

            assertThat(state.group("10:1").orElseThrow().status()).isEqualTo("BUNCHING");
            assertThat(state.staleCount()).isEqualTo(1);
        }

        /** Update mode also re-emits the *same* window with better data. That one must land. */
        @Test
        @DisplayName("a refined re-emission of the same window is accepted")
        void sameWindowIsRefinement() {
            state.accept(at("10:1", NOW, "ON_SCHEDULE", 1.0));

            assertThat(state.accept(at("10:1", NOW, "BUNCHING", 0.4))).isTrue();
            assertThat(state.group("10:1").orElseThrow().status()).isEqualTo("BUNCHING");
        }

        @Test
        @DisplayName("worst routes sort first, and routes with no verdict sort last")
        void ordering() {
            state.accept(at("a:0", NOW, "ON_SCHEDULE", 1.05));
            state.accept(at("b:0", NOW, "SEVERE_BUNCHING", 0.1));
            state.accept(at("c:0", NOW, "GAPPING", 1.7));
            state.accept(at("d:0", NOW, null, null));

            assertThat(state.current()).extracting(RouteHeadway::headwayGroup)
                    .containsExactly("b:0", "c:0", "a:0", "d:0");
        }

        @Test
        @DisplayName("routes that stop reporting are forgotten")
        void eviction() {
            state.accept(at("10:1", NOW.minusSeconds(3600), "ON_SCHEDULE", 1.0));
            state.accept(at("20:0", NOW.minusSeconds(30), "ON_SCHEDULE", 1.0));

            assertThat(state.evictStale(NOW, Duration.ofMinutes(10))).isEqualTo(1);
            assertThat(state.current()).extracting(RouteHeadway::headwayGroup)
                    .containsExactly("20:0");
        }

        /**
         * A {@code ConcurrentHashMap} throws on a null key, so a record missing its group would
         * take down the consumer thread rather than being skipped.
         */
        @Test
        @DisplayName("a record with no group or no window is dropped, not stored under null")
        void rejectsIncomplete() {
            RouteHeadway noGroup = new RouteHeadway(NOW, NOW, null, "10", "R", 0, null, 2,
                    null, null, null, null, List.of(), List.of(), List.of(), List.of(),
                    null, null, null, null, null, null, null);
            RouteHeadway noWindow = new RouteHeadway(NOW, null, "10:1", "10", "R", 0, null, 2,
                    null, null, null, null, List.of(), List.of(), List.of(), List.of(),
                    null, null, null, null, null, null, null);

            assertThat(state.accept(null)).isFalse();
            assertThat(state.accept(noGroup)).isFalse();
            assertThat(state.accept(noWindow)).isFalse();
            assertThat(state.size()).isZero();
        }

        /**
         * There is only one consumer thread today, so this cannot fail on the current wiring — but
         * the class must not silently become wrong the day a second topic or a partition-parallel
         * consumer is added. With a plain {@code if (newer) put(...)} some of these threads
         * interleave their read and their write and an older window survives.
         */
        @Test
        @DisplayName("concurrent writers still converge on the newest window")
        void concurrentWritersConverge() throws Exception {
            int threads = 8;
            int perThread = 500;
            ExecutorService pool = Executors.newFixedThreadPool(threads);
            CountDownLatch go = new CountDownLatch(1);

            for (int t = 0; t < threads; t++) {
                pool.submit(() -> {
                    go.await();
                    for (int i = 0; i < perThread; i++) {
                        state.accept(at("10:1", NOW.plusSeconds(i), "ON_SCHEDULE", 1.0));
                    }
                    return null;
                });
            }
            go.countDown();
            pool.shutdown();
            assertThat(pool.awaitTermination(20, TimeUnit.SECONDS)).isTrue();

            assertThat(state.group("10:1").orElseThrow().windowEnd())
                    .isEqualTo(NOW.plusSeconds(perThread - 1));
        }
    }

    @Nested
    @DisplayName("vehicles")
    class Vehicles {

        private final VehicleState state = new VehicleState();

        private VehiclePosition bus(String id, String route, Instant at) {
            return new VehiclePosition(id, route, "t" + id, 5, 33.75, -84.39, 180f, 8f, at);
        }

        @Test
        @DisplayName("a newer ping replaces an older one for the same bus")
        void newestWins() {
            state.accept(bus("4663", "10", NOW));
            state.accept(bus("4663", "10", NOW.plusSeconds(15)));

            assertThat(state.size()).isEqualTo(1);
            assertThat(state.all().iterator().next().timestamp()).isEqualTo(NOW.plusSeconds(15));
        }

        /**
         * Replaying the topic must reach the same state as a live tail, or restarting the API with
         * {@code earliest} offsets would leave the map showing whichever record happened to be last
         * in the log rather than the most recent one.
         */
        @Test
        @DisplayName("replaying the same records changes nothing")
        void idempotentUnderReplay() {
            List<VehiclePosition> batch = List.of(
                    bus("1", "10", NOW), bus("1", "10", NOW.plusSeconds(15)),
                    bus("2", "10", NOW.plusSeconds(5)));

            batch.forEach(state::accept);
            int afterFirst = state.size();
            Instant newest = state.all().stream().map(VehiclePosition::timestamp)
                    .max(Instant::compareTo).orElseThrow();

            batch.forEach(state::accept);
            batch.forEach(state::accept);

            assertThat(state.size()).isEqualTo(afterFirst);
            assertThat(state.all().stream().map(VehiclePosition::timestamp)
                    .max(Instant::compareTo).orElseThrow()).isEqualTo(newest);
            assertThat(state.ignoredCount()).isEqualTo(6);
        }

        @Test
        @DisplayName("buses can be filtered to one route, and stale ones drop off the map")
        void filterAndEvict() {
            state.accept(bus("1", "10", NOW));
            state.accept(bus("2", "51", NOW));
            state.accept(bus("3", "10", NOW.minusSeconds(3600)));

            assertThat(state.onRoute("10")).hasSize(2);
            assertThat(state.evictStale(NOW, Duration.ofMinutes(5))).isEqualTo(1);
            assertThat(state.onRoute("10")).hasSize(1);
        }
    }
}
