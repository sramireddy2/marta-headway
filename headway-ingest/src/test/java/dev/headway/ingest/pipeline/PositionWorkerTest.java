package dev.headway.ingest.pipeline;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import dev.headway.common.VehiclePosition;
import dev.headway.ingest.VehicleStore;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** See {@link ShardedPositionQueueTest} for why these carry a hard timeout. */
@Timeout(value = 30, unit = TimeUnit.SECONDS)
class PositionWorkerTest {

    private static final Instant T0 = Instant.parse("2026-08-14T21:00:00Z");

    private static VehiclePosition ping(String vehicleId, String routeId, Instant at) {
        return new VehiclePosition(vehicleId, routeId, "trip-1", 0, 33.75, -84.39, null, null, at);
    }

    private static VehicleStore storeAtT0() {
        return new VehicleStore(Clock.fixed(T0, ZoneOffset.UTC), Duration.ofMinutes(10));
    }

    @Test
    @DisplayName("a worker moves positions from its shard into the store and the publisher")
    void processesItsShard() throws Exception {
        IngestMetrics metrics = new IngestMetrics();
        ShardedPositionQueue queue = new ShardedPositionQueue(1, 32, metrics);
        VehicleStore store = storeAtT0();
        ConcurrentLinkedQueue<VehiclePosition> published = new ConcurrentLinkedQueue<>();

        PositionWorker worker = new PositionWorker(0, queue, store, published::add, metrics);

        try (ExecutorService pool = Executors.newSingleThreadExecutor()) {
            pool.submit(worker);

            queue.put(ping("bus-1", "15", T0));
            queue.put(ping("bus-2", "15", T0));

            await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
                assertThat(store.size()).isEqualTo(2);
                assertThat(published).hasSize(2);
                assertThat(metrics.processedTotal()).isEqualTo(2);
            });

            worker.stop();
        }
    }

    /**
     * Duplicates are dropped by the store but still published.
     *
     * <p>Kafka is the durable record of what the agency said; the store is our derived view of
     * current state. Filtering the topic down to only the readings that changed the view would
     * make it unreplayable — a consumer starting from offset zero would rebuild a different state
     * than one that had been running all along.
     */
    @Test
    @DisplayName("a duplicate is stale in the store but still reaches Kafka")
    void publishesEvenWhatTheStoreIgnores() throws Exception {
        IngestMetrics metrics = new IngestMetrics();
        ShardedPositionQueue queue = new ShardedPositionQueue(1, 32, metrics);
        VehicleStore store = storeAtT0();
        ConcurrentLinkedQueue<VehiclePosition> published = new ConcurrentLinkedQueue<>();

        PositionWorker worker = new PositionWorker(0, queue, store, published::add, metrics);

        try (ExecutorService pool = Executors.newSingleThreadExecutor()) {
            pool.submit(worker);

            VehiclePosition same = ping("bus-1", "15", T0);
            queue.put(same);
            queue.put(same);
            queue.put(same);

            await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
                assertThat(published).as("every delivery is published").hasSize(3);
                assertThat(store.size()).as("but the store holds one vehicle").isEqualTo(1);
                assertThat(metrics.staleTotal()).isEqualTo(2);
            });

            worker.stop();
        }
    }

    /**
     * Shutdown must not silently discard accepted work.
     *
     * <p>A worker that exits the moment {@code stop()} is called would abandon whatever is still
     * queued — positions we already took off the network and promised to handle.
     */
    @Test
    @DisplayName("stop() drains the shard before the worker exits")
    void drainsBeforeStopping() throws Exception {
        IngestMetrics metrics = new IngestMetrics();
        ShardedPositionQueue queue = new ShardedPositionQueue(1, 128, metrics);
        VehicleStore store = storeAtT0();
        ConcurrentLinkedQueue<VehiclePosition> published = new ConcurrentLinkedQueue<>();

        PositionWorker worker = new PositionWorker(0, queue, store, published::add, metrics);

        for (int i = 0; i < 100; i++) {
            queue.put(ping("bus-" + i, "15", T0));
        }
        // Ask it to stop before it has even started, so `running` is false on the first check.
        worker.stop();

        try (ExecutorService pool = Executors.newSingleThreadExecutor()) {
            pool.submit(worker);
            pool.shutdown();
            assertThat(pool.awaitTermination(15, TimeUnit.SECONDS)).isTrue();
        }

        assertThat(published).as("nothing accepted may be dropped at shutdown").hasSize(100);
        assertThat(queue.isEmpty()).isTrue();
    }

    @Test
    @DisplayName("a publisher that throws does not kill the worker")
    void survivesADownstreamFailure() throws Exception {
        IngestMetrics metrics = new IngestMetrics();
        ShardedPositionQueue queue = new ShardedPositionQueue(1, 32, metrics);
        VehicleStore store = storeAtT0();
        ConcurrentLinkedQueue<VehiclePosition> published = new ConcurrentLinkedQueue<>();

        PositionWorker worker = new PositionWorker(0, queue, store, position -> {
            if (position.vehicleId().equals("poison")) {
                throw new IllegalStateException("downstream blew up");
            }
            published.add(position);
        }, metrics);

        try (ExecutorService pool = Executors.newSingleThreadExecutor()) {
            pool.submit(worker);

            queue.put(ping("poison", "15", T0));
            queue.put(ping("bus-after", "15", T0));

            await().atMost(5, TimeUnit.SECONDS).untilAsserted(() ->
                    assertThat(published).extracting(VehiclePosition::vehicleId)
                            .containsExactly("bus-after"));

            worker.stop();
        }
    }

    /**
     * Four workers hammering one store concurrently — the situation
     * {@code ConcurrentHashMap.compute} was chosen for back in step 2, now actually happening.
     */
    @Test
    @DisplayName("four workers on four shards leave the store consistent")
    void workersShareTheStoreSafely() throws Exception {
        int shards = 4;
        int perRoute = 200;
        List<String> routes = List.of("15", "110", "39", "2", "816", "89");

        IngestMetrics metrics = new IngestMetrics();
        ShardedPositionQueue queue = new ShardedPositionQueue(shards, 64, metrics);
        VehicleStore store = new VehicleStore(
                Clock.fixed(T0.plusSeconds(perRoute), ZoneOffset.UTC), Duration.ofDays(1));

        List<PositionWorker> workers = java.util.stream.IntStream.range(0, shards)
                .mapToObj(s -> new PositionWorker(s, queue, store, p -> { }, metrics))
                .toList();

        try (ExecutorService pool = Executors.newFixedThreadPool(shards)) {
            workers.forEach(pool::submit);

            for (int i = 0; i < perRoute; i++) {
                for (String route : routes) {
                    queue.put(ping(route + "-bus", route, T0.plusSeconds(i)));
                }
            }

            await().atMost(20, TimeUnit.SECONDS).untilAsserted(() ->
                    assertThat(metrics.processedTotal()).isEqualTo((long) perRoute * routes.size()));

            workers.forEach(PositionWorker::stop);
        }

        assertThat(store.size()).as("one entry per vehicle").isEqualTo(routes.size());
        assertThat(store.snapshot().values()).allSatisfy(vp ->
                assertThat(vp.timestamp())
                        .as("the newest reading must win for every route")
                        .isEqualTo(T0.plusSeconds(perRoute - 1)));
    }
}
