package dev.headway.ingest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIOException;

import dev.headway.common.VehiclePosition;
import dev.headway.ingest.pipeline.IngestMetrics;
import dev.headway.ingest.pipeline.ShardedPositionQueue;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class FeedPollerTest {

    private static final Instant T0 = Instant.parse("2026-08-14T21:00:00Z");

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

    private static ShardedPositionQueue queue(IngestMetrics metrics) {
        return new ShardedPositionQueue(2, 512, metrics);
    }

    @Test
    @DisplayName("a poll puts every decoded position on the queue")
    void pollEnqueuesEverything() throws Exception {
        IngestMetrics metrics = new IngestMetrics();
        ShardedPositionQueue queue = queue(metrics);
        FakeFeed feed = new FakeFeed().returning(List.of(ping("bus-1", T0), ping("bus-2", T0)));

        FeedPoller poller = new FeedPoller("test", feed, queue, 1000, metrics);
        FeedPoller.Result result = poller.pollOnce();

        assertThat(result.received()).isEqualTo(2);
        assertThat(queue.depth()).isEqualTo(2);
        assertThat(metrics.fetchedTotal()).isEqualTo(2);
        assertThat(metrics.enqueuedTotal()).isEqualTo(2);
    }

    @Test
    @DisplayName("with room in the queue the poller never blocks")
    void doesNotBlockWhenThereIsRoom() throws Exception {
        IngestMetrics metrics = new IngestMetrics();
        FeedPoller poller = new FeedPoller("test",
                new FakeFeed().returning(List.of(ping("bus-1", T0))), queue(metrics), 1000, metrics);

        assertThat(poller.pollOnce().blockedMillis()).isZero();
        assertThat(metrics.totalEnqueueWaitMillis()).isZero();
    }

    /**
     * The poller is the producer side of backpressure. When the queue has no room it must wait,
     * not drop and not grow the queue.
     */
    @Test
    @DisplayName("a full queue makes the poller wait rather than drop positions")
    void blocksWhenTheQueueIsFull() throws Exception {
        IngestMetrics metrics = new IngestMetrics();
        ShardedPositionQueue tiny = new ShardedPositionQueue(1, 2, metrics);
        FakeFeed feed = new FakeFeed().returning(
                List.of(ping("bus-1", T0), ping("bus-2", T0), ping("bus-3", T0)));
        FeedPoller poller = new FeedPoller("test", feed, tiny, 1000, metrics);

        // Free a slot shortly after the poll starts, so the third put unblocks.
        Thread reader = new Thread(() -> {
            try {
                Thread.sleep(60);
                tiny.poll(0, 1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        reader.start();

        FeedPoller.Result result = poller.pollOnce();
        reader.join(2000);

        assertThat(result.blockedMillis()).as("the poller was held back").isPositive();
        assertThat(tiny.depth()).as("the bound held throughout").isLessThanOrEqualTo(2);
    }

    @Test
    @DisplayName("pollOnce propagates failures so callers can react")
    void pollOnceThrows() {
        IngestMetrics metrics = new IngestMetrics();
        FakeFeed feed = new FakeFeed().failing(new IOException("feed returned HTTP 503"));
        FeedPoller poller = new FeedPoller("test", feed, queue(metrics), 1000, metrics);

        assertThatIOException().isThrownBy(poller::pollOnce).withMessageContaining("503");
        assertThat(poller.pollsFailedTotal()).isEqualTo(1);
        assertThat(poller.consecutiveFailures()).isEqualTo(1);
        assertThat(metrics.registry().get("headway.poll.failures").counter().count()).isEqualTo(1);
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
        IngestMetrics metrics = new IngestMetrics();
        FakeFeed feed = new FakeFeed().failing(new IOException("network down"));
        FeedPoller poller = new FeedPoller("test", feed, queue(metrics), 1000, metrics);

        for (int i = 0; i < 3; i++) {
            poller.run(); // must not throw
        }

        assertThat(poller.pollsFailedTotal()).isEqualTo(3);
        assertThat(poller.consecutiveFailures()).isEqualTo(3);
    }

    @Test
    @DisplayName("the failure streak resets after a good poll")
    void recoveryResetsTheStreak() throws Exception {
        IngestMetrics metrics = new IngestMetrics();
        FakeFeed feed = new FakeFeed().failing(new IOException("down"));
        FeedPoller poller = new FeedPoller("test", feed, queue(metrics), 1000, metrics);

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
        IngestMetrics metrics = new IngestMetrics();
        FakeFeed feed = new FakeFeed().returning(List.of(ping("bus-1", T0)));
        FeedPoller poller = new FeedPoller("test", feed, queue(metrics), 20.0, metrics);

        poller.pollOnce();
        long start = System.nanoTime();
        poller.pollOnce();
        long elapsedMillis = (System.nanoTime() - start) / 1_000_000;

        // Generous lower bound: we care that it waited, not exactly how long.
        assertThat(elapsedMillis).as("second poll should have been throttled").isGreaterThan(30);
    }
}
