package dev.headway.ingest.pipeline;

import static org.assertj.core.api.Assertions.assertThat;

import dev.headway.common.VehiclePosition;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Concurrency tests get a hard timeout. A bug in code that blocks does not produce a failure, it
 * produces a hang — and a hung build tells you nothing and costs ten minutes. Failing at 30
 * seconds turns a deadlock into an ordinary red test with a stack trace.
 */
@Timeout(value = 30, unit = TimeUnit.SECONDS)
class ShardedPositionQueueTest {

    private static final Instant T0 = Instant.parse("2026-08-14T21:00:00Z");

    private static VehiclePosition ping(String vehicleId, String routeId, Instant at) {
        return new VehiclePosition(vehicleId, routeId, "trip-1", 0, 33.75, -84.39, null, null, at);
    }

    @Test
    @DisplayName("a route always maps to the same shard")
    void routeToShardIsStable() {
        ShardedPositionQueue queue = new ShardedPositionQueue(4, 16, new IngestMetrics());

        int first = queue.shardFor("15");
        for (int i = 0; i < 1000; i++) {
            assertThat(queue.shardFor("15")).isEqualTo(first);
        }
        assertThat(first).isBetween(0, 3);
    }

    /**
     * {@code hashCode()} is often negative, and {@code -7 % 4} is {@code -3} in Java — a negative
     * array index. Numeric route ids like "15" always hash positive, so this bug hides completely
     * behind MARTA's own data and would surface the day someone adds a named route.
     *
     * <p>The last entry is the one worth knowing. {@code "polygenelubricants".hashCode()} is
     * exactly {@link Integer#MIN_VALUE}, and {@code Math.abs(Integer.MIN_VALUE)} is <em>still</em>
     * {@code Integer.MIN_VALUE} — negation overflows. So the usual workaround,
     * {@code Math.abs(hash) % n}, is also broken, and broken for a value no amount of testing with
     * ordinary strings will ever produce. {@link Math#floorMod} is the correct tool.
     */
    @Test
    @DisplayName("negative hash codes never produce a negative shard index")
    void handlesNegativeHashCodes() {
        ShardedPositionQueue queue = new ShardedPositionQueue(4, 16, new IngestMetrics());

        List<String> routes = new ArrayList<>(List.of(
                "MARTA-Red-Line-Northbound",   // -729652972
                "Gold Line to Doraville",      // -1555548929
                "hierarchically",              // -444805418
                "polygenelubricants",          // Integer.MIN_VALUE
                "15", "110", "816", ""));
        IntStream.range(0, 500).forEach(i -> routes.add("route-" + i));

        assertThat(routes.stream().filter(r -> r.hashCode() < 0).count())
                .as("the sample must really contain negative hashes or this test proves nothing")
                .isGreaterThanOrEqualTo(4);

        assertThat(routes).allSatisfy(r -> assertThat(queue.shardFor(r)).isBetween(0, 3));

        // Spell out why the obvious alternative is not good enough.
        assertThat(Math.abs("polygenelubricants".hashCode()))
                .as("Math.abs cannot fix Integer.MIN_VALUE; floorMod is required")
                .isNegative();
    }

    @Test
    @DisplayName("routes spread across shards rather than piling into one")
    void routesSpreadAcrossShards() {
        ShardedPositionQueue queue = new ShardedPositionQueue(4, 16, new IngestMetrics());
        List<String> martaRoutes = List.of("1", "2", "3", "5", "15", "39", "89", "110", "121", "186");

        assertThat(martaRoutes.stream().map(queue::shardFor).distinct().count())
                .isGreaterThan(1);
    }

    /**
     * The test this whole step exists for.
     *
     * <p>A tiny queue and a deliberately slow consumer. The producer tries to push far more than
     * fits. Two things must be true: the producer is <em>held back</em> (measurable blocked time),
     * and the queue never exceeds its bound no matter how far ahead the producer gets.
     *
     * <p>The second assertion is the one that matters. With an unbounded queue the producer would
     * never block, the test would "pass" faster, and the queue would have grown to 200 items — the
     * beginning of the curve that ends in {@code OutOfMemoryError} under real load.
     */
    @Test
    @DisplayName("a full queue blocks the producer and never grows past its bound")
    void backpressureBlocksTheProducer() throws Exception {
        int capacity = 4;
        int itemCount = 40;
        ShardedPositionQueue queue = new ShardedPositionQueue(1, capacity, new IngestMetrics());

        AtomicInteger consumed = new AtomicInteger();
        AtomicInteger maxDepthSeen = new AtomicInteger();
        CountDownLatch consumerReady = new CountDownLatch(1);

        try (ExecutorService pool = Executors.newFixedThreadPool(2)) {
            // Slow consumer: 5ms per item, so the producer cannot possibly keep up.
            pool.submit(() -> {
                consumerReady.countDown();
                try {
                    while (consumed.get() < itemCount) {
                        VehiclePosition p = queue.poll(0, 500, TimeUnit.MILLISECONDS);
                        if (p != null) {
                            Thread.sleep(5);
                            consumed.incrementAndGet();
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });

            assertThat(consumerReady.await(5, TimeUnit.SECONDS)).isTrue();

            AtomicLong totalBlockedNanos = new AtomicLong();
            for (int i = 0; i < itemCount; i++) {
                totalBlockedNanos.addAndGet(queue.put(ping("bus-" + i, "15", T0.plusSeconds(i))));
                maxDepthSeen.updateAndGet(prev -> Math.max(prev, queue.depth()));
            }

            assertThat(totalBlockedNanos.get())
                    .as("producer must have been held back by the slow consumer")
                    .isPositive();

            assertThat(maxDepthSeen.get())
                    .as("the bound must hold; this is what stops an OutOfMemoryError")
                    .isLessThanOrEqualTo(capacity);

            // And nothing was lost while all that blocking happened.
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (consumed.get() < itemCount && System.nanoTime() < deadline) {
                Thread.sleep(10);
            }
            assertThat(consumed.get()).isEqualTo(itemCount);
        }
    }

    @Test
    @DisplayName("blocked time is recorded so backpressure is visible in metrics")
    void backpressureIsMeasured() throws Exception {
        IngestMetrics metrics = new IngestMetrics();
        ShardedPositionQueue queue = new ShardedPositionQueue(1, 2, metrics);

        queue.put(ping("bus-1", "15", T0));
        queue.put(ping("bus-2", "15", T0));
        assertThat(metrics.totalEnqueueWaitMillis()).as("no waiting while there was room").isZero();

        // Third put must block until something is taken.
        try (ExecutorService pool = Executors.newSingleThreadExecutor()) {
            pool.submit(() -> {
                try {
                    Thread.sleep(60);
                    queue.poll(0, 1, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });

            long blocked = queue.put(ping("bus-3", "15", T0));

            assertThat(blocked).isPositive();
            assertThat(metrics.totalEnqueueWaitMillis()).isGreaterThan(30);
        }
    }

    /**
     * The ordering guarantee that justifies sharding instead of one shared queue.
     *
     * <p>Everything for one route must come out in the order it went in, because that route only
     * ever reaches one worker. With a single queue and four workers this property does not hold,
     * and Kafka's per-route ordering — the whole point of step 3's partition key — breaks.
     */
    @Test
    @DisplayName("positions for one route come out in the order they went in")
    void preservesPerRouteOrder() throws Exception {
        int perRoute = 50;
        List<String> routes = List.of("15", "110", "39", "2", "816");

        // Nothing drains while we fill, so every shard must be able to hold the whole workload:
        // several routes can hash to the same shard, and a shard too small to fit them would
        // block put() forever. That is backpressure behaving correctly against a test with no
        // consumer — worth knowing, because it is an easy way to deadlock your own test.
        ShardedPositionQueue queue =
                new ShardedPositionQueue(4, perRoute * routes.size(), new IngestMetrics());

        for (int i = 0; i < perRoute; i++) {
            for (String route : routes) {
                queue.put(ping(route + "-bus-" + i, route, T0.plusSeconds(i)));
            }
        }

        // Drain each shard exactly the way a worker does: one thread per shard, in order.
        List<List<VehiclePosition>> drained = new ArrayList<>();
        for (int shard = 0; shard < queue.shardCount(); shard++) {
            List<VehiclePosition> out = new ArrayList<>();
            VehiclePosition p;
            while ((p = queue.poll(shard, 50, TimeUnit.MILLISECONDS)) != null) {
                out.add(p);
            }
            drained.add(out);
        }

        for (String route : routes) {
            List<Instant> seen = drained.get(queue.shardFor(route)).stream()
                    .filter(p -> p.routeId().equals(route))
                    .map(VehiclePosition::timestamp)
                    .toList();

            assertThat(seen).hasSize(perRoute);
            assertThat(seen).as("route %s must stay in order", route).isSorted();
        }
    }

    @Test
    @DisplayName("utilization reports how close to the bound we are")
    void reportsUtilization() throws Exception {
        ShardedPositionQueue queue = new ShardedPositionQueue(2, 10, new IngestMetrics());
        assertThat(queue.utilization()).isZero();
        assertThat(queue.totalCapacity()).isEqualTo(20);

        for (int i = 0; i < 10; i++) {
            queue.put(ping("bus-" + i, "15", T0.plusSeconds(i)));
        }

        assertThat(queue.depth()).isEqualTo(10);
        assertThat(queue.utilization()).isEqualTo(0.5);
    }
}
