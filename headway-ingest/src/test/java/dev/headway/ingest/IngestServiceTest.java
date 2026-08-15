package dev.headway.ingest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import dev.headway.common.VehiclePosition;
import dev.headway.ingest.pipeline.IngestMetrics;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Lifecycle tests for the whole pipeline: start it, drive real traffic through it, shut it down,
 * and check that the number of positions that went in equals the number that came out.
 *
 * <p>This is the test that covers what a user actually does — Ctrl+C — without depending on
 * operating-system signal delivery. The shutdown hook itself is three lines that call
 * {@link IngestService#close()}; everything that can go wrong lives in the ordering inside
 * {@code close()}, and that is what is exercised here.
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class IngestServiceTest {

    /** Emits a fresh batch of current-timestamped positions on every call. */
    private static final class SyntheticFeed implements VehicleFeed {
        private final int batchSize;
        private final AtomicInteger polls = new AtomicInteger();

        SyntheticFeed(int batchSize) {
            this.batchSize = batchSize;
        }

        @Override
        public List<VehiclePosition> fetchVehiclePositions() {
            int poll = polls.incrementAndGet();
            Instant now = Instant.now();
            List<VehiclePosition> batch = new ArrayList<>(batchSize);
            for (int i = 0; i < batchSize; i++) {
                batch.add(new VehiclePosition(
                        "bus-" + i, "route-" + (i % 13), "trip-" + poll, 0,
                        33.75, -84.39, null, null, now.plusMillis(poll)));
            }
            return batch;
        }

        int polls() {
            return polls.get();
        }
    }

    private static IngestService.Config fastConfig(int shards, int capacity) {
        return new IngestService.Config(
                Duration.ofMillis(50),      // poll fast; this is a fake feed, nobody to be polite to
                1000.0,                     // rate limiter effectively off
                Duration.ofMinutes(10),
                Duration.ofHours(1),        // no eviction during the test
                shards, capacity,
                Duration.ofHours(1));       // no metrics chatter
    }

    @Test
    @DisplayName("nothing is lost across a full start-run-stop lifecycle")
    void losesNothingAcrossLifecycle() {
        IngestMetrics metrics = new IngestMetrics();
        SyntheticFeed feed = new SyntheticFeed(50);
        ConcurrentLinkedQueue<VehiclePosition> published = new ConcurrentLinkedQueue<>();

        IngestService service =
                new IngestService(feed, fastConfig(4, 64), published::add, metrics);
        service.start();

        await().atMost(20, TimeUnit.SECONDS)
                .until(() -> metrics.processedTotal() >= 250);

        service.close();

        assertThat(service.queue().isEmpty()).as("queue drained").isTrue();
        assertThat(metrics.enqueuedTotal())
                .as("every fetched position was enqueued")
                .isEqualTo(metrics.fetchedTotal());
        assertThat(metrics.processedTotal())
                .as("every enqueued position was processed")
                .isEqualTo(metrics.enqueuedTotal());
        assertThat(published)
                .as("every processed position was published")
                .hasSize((int) metrics.processedTotal());
        assertThat(feed.polls()).isPositive();
    }

    /**
     * The ordering inside {@code close()} is the whole point.
     *
     * <p>A slow publisher guarantees there is a real backlog at the moment shutdown starts. If
     * {@code close()} stopped workers before letting them drain — or stopped them before the
     * in-flight fetch finished enqueuing — those queued positions would vanish silently. They were
     * already taken off the network, so losing them is real data loss, not a dropped connection.
     */
    @Test
    @DisplayName("shutdown drains a backlog instead of discarding it")
    void shutdownDrainsBacklog() {
        IngestMetrics metrics = new IngestMetrics();
        SyntheticFeed feed = new SyntheticFeed(60);
        ConcurrentLinkedQueue<VehiclePosition> published = new ConcurrentLinkedQueue<>();

        // 1ms per position is far slower than the feed produces them, so a backlog is guaranteed.
        IngestService service = new IngestService(feed, fastConfig(2, 32), position -> {
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            published.add(position);
        }, metrics);

        service.start();

        // Wait until the queue genuinely has work sitting in it.
        await().atMost(20, TimeUnit.SECONDS).until(() -> service.queue().depth() > 0);
        int backlogAtShutdown = service.queue().depth();

        service.close();

        assertThat(backlogAtShutdown).as("test is only meaningful with a real backlog").isPositive();
        assertThat(service.queue().isEmpty()).as("backlog was drained, not dropped").isTrue();
        assertThat(published).hasSize((int) metrics.enqueuedTotal());
    }

    @Test
    @DisplayName("close() returns promptly when there is nothing in flight")
    void closeIsPromptWhenIdle() {
        IngestMetrics metrics = new IngestMetrics();
        IngestService service = new IngestService(
                new SyntheticFeed(5), fastConfig(2, 32), position -> { }, metrics);
        service.start();

        await().atMost(10, TimeUnit.SECONDS).until(() -> metrics.processedTotal() > 0);

        long start = System.nanoTime();
        service.close();
        long millis = (System.nanoTime() - start) / 1_000_000;

        assertThat(millis).as("an idle shutdown must not sit through every timeout").isLessThan(5_000);
    }

    /** Same reasoning as {@code KafkaPositionPublisherTest.closeIsIdempotent} — the Ctrl+C path. */
    @Test
    @DisplayName("close() twice is safe and does not re-run the shutdown sequence")
    void closeIsIdempotent() {
        IngestMetrics metrics = new IngestMetrics();
        IngestService service = new IngestService(
                new SyntheticFeed(10), fastConfig(2, 32), position -> { }, metrics);
        service.start();

        await().atMost(10, TimeUnit.SECONDS).until(() -> metrics.processedTotal() > 0);

        service.close();
        long processedAfterFirstClose = metrics.processedTotal();

        service.close(); // must not throw, must not block
        service.close();

        assertThat(metrics.processedTotal()).isEqualTo(processedAfterFirstClose);
    }

    @Test
    @DisplayName("the store and queue gauges are registered and readable")
    void exposesGauges() {
        IngestMetrics metrics = new IngestMetrics();
        IngestService service = new IngestService(
                new SyntheticFeed(10), fastConfig(2, 32), position -> { }, metrics);

        try {
            service.start();
            await().atMost(10, TimeUnit.SECONDS).until(() -> metrics.processedTotal() > 0);

            assertThat(metrics.registry().get("headway.queue.capacity").gauge().value())
                    .isEqualTo(64.0);
            assertThat(metrics.registry().get("headway.store.vehicles").gauge().value())
                    .isPositive();
            assertThat(metrics.registry().get("headway.queue.utilization").gauge().value())
                    .isBetween(0.0, 1.0);
        } finally {
            service.close();
        }
    }
}
