package dev.headway.ingest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIOException;

import dev.headway.common.VehiclePosition;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class FeedPollerTest {

    private static final Instant T0 = Instant.parse("2026-08-14T21:00:00Z");

    /**
     * A store whose clock is frozen at {@code T0}, so the fixed-date pings below are always
     * considered current no matter when the suite runs.
     */
    private static VehicleStore newStore() {
        return new VehicleStore(Clock.fixed(T0, ZoneOffset.UTC), Duration.ofMinutes(10));
    }

    private static VehiclePosition ping(String vehicleId, Instant at) {
        return new VehiclePosition(vehicleId, "15", "trip-1", 0, 33.75, -84.39, null, null, at);
    }

    /** A hand-written fake. No mocking framework needed when the interface has one method. */
    private static final class FakeFeed implements VehicleFeed {
        private final List<List<VehiclePosition>> batches = new ArrayList<>();
        private final AtomicInteger call = new AtomicInteger();
        private IOException failWith;

        FakeFeed returning(List<VehiclePosition> batch) {
            batches.add(batch);
            return this;
        }

        FakeFeed failing(IOException e) {
            this.failWith = e;
            return this;
        }

        @Override
        public List<VehiclePosition> fetchVehiclePositions() throws IOException {
            if (failWith != null) {
                throw failWith;
            }
            int i = call.getAndIncrement();
            return batches.get(Math.min(i, batches.size() - 1));
        }
    }

    @Test
    @DisplayName("a poll fills the store and forwards the batch downstream")
    void pollAppliesAndForwards() throws Exception {
        VehicleStore store = newStore();
        List<VehiclePosition> forwarded = new ArrayList<>();

        FakeFeed feed = new FakeFeed().returning(List.of(ping("bus-1", T0), ping("bus-2", T0)));
        FeedPoller poller = new FeedPoller(feed, store, 1000, forwarded::addAll);

        FeedPoller.Result result = poller.pollOnce();

        assertThat(result.received()).isEqualTo(2);
        assertThat(result.stats().created()).isEqualTo(2);
        assertThat(store.size()).isEqualTo(2);
        assertThat(forwarded).hasSize(2);
    }

    @Test
    @DisplayName("polling the same unchanged feed twice creates nothing new")
    void repeatedPollsAreIdempotent() throws Exception {
        VehicleStore store = newStore();
        FakeFeed feed = new FakeFeed().returning(List.of(ping("bus-1", T0)));
        FeedPoller poller = new FeedPoller(feed, store, 1000, positions -> {});

        assertThat(poller.pollOnce().stats().created()).isEqualTo(1);
        assertThat(poller.pollOnce().stats().stale()).isEqualTo(1);
        assertThat(store.size()).isEqualTo(1);
    }

    @Test
    @DisplayName("pollOnce propagates failures so callers can react")
    void pollOnceThrows() {
        FakeFeed feed = new FakeFeed().failing(new IOException("feed returned HTTP 503"));
        FeedPoller poller = new FeedPoller(feed, newStore(), 1000, positions -> {});

        assertThatIOException().isThrownBy(poller::pollOnce).withMessageContaining("503");
        assertThat(poller.pollsFailedTotal()).isEqualTo(1);
        assertThat(poller.consecutiveFailures()).isEqualTo(1);
    }

    /**
     * The behaviour that keeps the service alive.
     *
     * <p>If {@code run()} let the exception escape, {@code scheduleWithFixedDelay} would cancel the
     * task forever and the app would go quiet with no error in the log.
     */
    @Test
    @DisplayName("run() swallows failures so the scheduler is never cancelled")
    void runNeverThrows() {
        FakeFeed feed = new FakeFeed().failing(new IOException("network down"));
        FeedPoller poller = new FeedPoller(feed, newStore(), 1000, positions -> {});

        for (int i = 0; i < 3; i++) {
            poller.run(); // must not throw
        }

        assertThat(poller.pollsFailedTotal()).isEqualTo(3);
        assertThat(poller.consecutiveFailures()).isEqualTo(3);
    }

    @Test
    @DisplayName("the failure streak resets after a good poll")
    void recoveryResetsTheStreak() throws Exception {
        VehicleStore store = newStore();
        FakeFeed feed = new FakeFeed().failing(new IOException("down"));
        FeedPoller poller = new FeedPoller(feed, store, 1000, positions -> {});

        poller.run();
        assertThat(poller.consecutiveFailures()).isEqualTo(1);

        feed.failWith = null;
        feed.returning(List.of(ping("bus-1", T0)));
        poller.pollOnce();

        assertThat(poller.consecutiveFailures()).isZero();
    }

    @Test
    @DisplayName("the rate limiter actually delays the second poll")
    void rateLimiterThrottles() throws Exception {
        // 20 permits/second means one every 50ms. Guava hands out the first acquire immediately,
        // so only the second one should wait.
        FakeFeed feed = new FakeFeed().returning(List.of(ping("bus-1", T0)));
        FeedPoller poller = new FeedPoller(feed, newStore(), 20.0, positions -> {});

        poller.pollOnce();
        long start = System.nanoTime();
        poller.pollOnce();
        long elapsedMillis = (System.nanoTime() - start) / 1_000_000;

        // Generous lower bound: we care that it waited, not exactly how long.
        assertThat(elapsedMillis).as("second poll should have been throttled").isGreaterThan(30);
    }
}
