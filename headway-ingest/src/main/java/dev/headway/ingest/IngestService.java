package dev.headway.ingest;

import dev.headway.common.VehiclePosition;
import dev.headway.ingest.pipeline.IngestMetrics;
import dev.headway.ingest.pipeline.PositionWorker;
import dev.headway.ingest.pipeline.ShardedPositionQueue;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Owns every thread in the ingest pipeline and the order in which they shut down.
 *
 * <pre>
 *   scheduler          fetch pool            queue (bounded, sharded)      workers
 *   1 platform thread  virtual threads       4 shards x 256                4 platform threads
 *        |                   |                        |                         |
 *   every 15s ------&gt; submit fetch -----&gt; put() (blocks when full) -----&gt; store + Kafka
 * </pre>
 *
 * <h2>Three different thread choices, on purpose</h2>
 *
 * <b>Scheduler — one platform thread.</b> Java 21 has no virtual-thread scheduled executor, and it
 * does not need one: this thread does nothing but wake up and submit. Keeping the timer off the
 * work pool is also what stops a slow fetch from delaying the next tick.
 *
 * <p><b>Fetches — virtual threads.</b> A fetch is almost entirely waiting on a socket. A platform
 * thread parked on I/O still costs a megabyte of stack and an OS scheduler slot; a virtual thread
 * parked on I/O costs a few hundred bytes of heap, because the JVM unmounts it from its carrier
 * thread while it waits. With one feed today the difference is academic and it would be dishonest
 * to claim otherwise — the point is that adding the trip-updates feed in step 5, and other
 * agencies after that, costs nothing.
 *
 * <p><b>Workers — a small fixed pool of platform threads.</b> This is the part people get wrong
 * after discovering virtual threads. Workers do CPU work (hashing, JSON encoding) on data already
 * in memory. Virtual threads make blocking cheap; they do not make computation faster, and a
 * virtual thread per task would just create unbounded concurrency over a bounded CPU. A fixed pool
 * sized to the shard count is right here, and the count is fixed by ordering anyway: exactly one
 * worker per shard.
 *
 * <p>The rule worth remembering: <b>virtual threads for waiting, platform threads for working.</b>
 */
public final class IngestService implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(IngestService.class);

    /** All the knobs in one place, with defaults that are polite to MARTA. */
    public record Config(
            Duration pollInterval,
            double maxRequestsPerSecond,
            Duration evictAfter,
            Duration evictionSweepInterval,
            int shardCount,
            int queueCapacityPerShard,
            Duration metricsInterval) {

        public static Config defaults() {
            return new Config(
                    Duration.ofSeconds(15),   // MARTA republishes roughly this often
                    1.0 / 15.0,               // ceiling: one request per 15 seconds
                    Duration.ofMinutes(10),   // a bus silent for 10 min has finished its run
                    Duration.ofMinutes(1),
                    4,                        // shards, and therefore workers
                    256,                      // per shard: ~1024 total, vs ~190 per poll
                    Duration.ofSeconds(30));
        }

        /**
         * Defaults, with the queue geometry overridable from the environment.
         *
         * <p>Exposed mainly so backpressure can be demonstrated on demand. At the default 1024
         * slots against ~190 positions per poll the queue never fills, which is correct but means
         * you never see the mechanism work. Shrink it and the producer starts blocking:
         *
         * <pre>{@code $env:HEADWAY_QUEUE_CAPACITY = "4"}</pre>
         */
        public static Config fromEnvironment() {
            Config base = defaults();
            return new Config(
                    base.pollInterval(),
                    base.maxRequestsPerSecond(),
                    base.evictAfter(),
                    base.evictionSweepInterval(),
                    intEnv("HEADWAY_QUEUE_SHARDS", base.shardCount()),
                    intEnv("HEADWAY_QUEUE_CAPACITY", base.queueCapacityPerShard()),
                    base.metricsInterval());
        }

        private static int intEnv(String name, int fallback) {
            String raw = System.getenv(name);
            if (raw == null || raw.isBlank()) {
                return fallback;
            }
            try {
                return Integer.parseInt(raw.trim());
            } catch (NumberFormatException e) {
                log.warn("{}='{}' is not a number; using {}", name, raw, fallback);
                return fallback;
            }
        }
    }

    private final Config config;
    private final IngestMetrics metrics;
    private final VehicleStore store;
    private final ShardedPositionQueue queue;
    private final FeedPoller poller;
    private final List<PositionWorker> workers = new ArrayList<>();

    private final ScheduledExecutorService scheduler;
    private final ExecutorService fetchExecutor;
    private final ExecutorService workerExecutor;
    private final java.util.concurrent.atomic.AtomicBoolean closed =
            new java.util.concurrent.atomic.AtomicBoolean();

    public IngestService(VehicleFeed feed, Config config, Consumer<VehiclePosition> publisher) {
        this(feed, config, publisher, new IngestMetrics());
    }

    public IngestService(VehicleFeed feed, Config config, Consumer<VehiclePosition> publisher,
                         IngestMetrics metrics) {
        this.config = config;
        this.metrics = metrics;
        this.store = new VehicleStore(config.evictAfter());
        this.queue = new ShardedPositionQueue(config.shardCount(), config.queueCapacityPerShard(), metrics);
        this.poller = new FeedPoller("vehicle-positions", feed, queue,
                config.maxRequestsPerSecond(), metrics);

        this.scheduler = Executors.newScheduledThreadPool(2, namedThreadFactory("headway-sched"));
        this.fetchExecutor = Executors.newThreadPerTaskExecutor(
                Thread.ofVirtual().name("headway-fetch-", 0).factory());
        this.workerExecutor = Executors.newFixedThreadPool(
                config.shardCount(), namedThreadFactory("headway-worker"));

        for (int shard = 0; shard < config.shardCount(); shard++) {
            workers.add(new PositionWorker(shard, queue, store, publisher, metrics));
        }

        registerGauges();
    }

    private void registerGauges() {
        metrics.gauge("headway.queue.depth", "Positions waiting across all shards", queue::depth);
        metrics.gauge("headway.queue.utilization", "Queue fullness, 0 to 1", queue::utilization);
        metrics.gauge("headway.queue.capacity", "Total bounded capacity", queue::totalCapacity);
        metrics.gauge("headway.store.vehicles", "Vehicles currently tracked", store::size);
        metrics.gauge("headway.store.routes", "Routes with a live vehicle", store::routeCount);
    }

    private static ThreadFactory namedThreadFactory(String prefix) {
        AtomicInteger counter = new AtomicInteger();
        return runnable -> {
            Thread t = new Thread(runnable, prefix + "-" + counter.incrementAndGet());
            // A daemon thread does not keep the JVM alive on its own. We want shutdown to be
            // driven by our own close(), not blocked by a stray timer thread.
            t.setDaemon(true);
            return t;
        };
    }

    /** Starts workers and polling. Returns immediately; work happens on the background threads. */
    public void start() {
        log.info("Starting ingest: poll every {}s, ceiling {} req/s, evict after {}min, "
                        + "{} shards x {} = {} queue slots",
                config.pollInterval().toSeconds(),
                "%.4f".formatted(config.maxRequestsPerSecond()),
                config.evictAfter().toMinutes(),
                config.shardCount(), config.queueCapacityPerShard(), queue.totalCapacity());

        // Workers first. Starting producers before consumers would fill the queue and report
        // backpressure that is purely an artefact of startup order.
        workers.forEach(workerExecutor::submit);

        // The scheduler only *submits*; the fetch itself runs on a virtual thread. That keeps the
        // timer thread free, so a fetch that overruns cannot delay eviction or the next tick.
        scheduler.scheduleWithFixedDelay(
                () -> fetchExecutor.submit(poller),
                0, config.pollInterval().toMillis(), TimeUnit.MILLISECONDS);

        scheduler.scheduleWithFixedDelay(this::sweep,
                config.evictionSweepInterval().toMillis(),
                config.evictionSweepInterval().toMillis(), TimeUnit.MILLISECONDS);

        scheduler.scheduleWithFixedDelay(this::reportMetrics,
                config.metricsInterval().toMillis(),
                config.metricsInterval().toMillis(), TimeUnit.MILLISECONDS);
    }

    private void sweep() {
        try {
            int removed = store.evictStale();
            if (removed > 0) {
                log.info("Evicted {} stale vehicles; {} remain", removed, store.size());
            }
        } catch (Throwable t) {
            // Same reason as FeedPoller.run(): a throw here would cancel the sweep forever.
            log.error("Eviction sweep failed. Continuing.", t);
        }
    }

    /** The line that makes the pipeline's health legible at a glance. */
    private void reportMetrics() {
        try {
            log.info("metrics | fetched {} -> enqueued {} -> processed {} ({} stale, {} rejected) "
                            + "| queue {}/{} {} depths={} | blocked {}ms total "
                            + "| kafka {} sent / {} failed | store {} vehicles on {} routes",
                    metrics.fetchedTotal(), metrics.enqueuedTotal(), metrics.processedTotal(),
                    metrics.staleTotal(), metrics.rejectedTotal(),
                    queue.depth(), queue.totalCapacity(),
                    "%.0f%%".formatted(queue.utilization() * 100), queue.depths(),
                    "%.0f".formatted(metrics.totalEnqueueWaitMillis()),
                    metrics.kafkaSentTotal(), metrics.kafkaFailedTotal(),
                    store.size(), store.routeCount());
        } catch (Throwable t) {
            log.error("Metrics report failed. Continuing.", t);
        }
    }

    /**
     * Stops everything in the one order that loses no data.
     *
     * <ol>
     *   <li><b>Scheduler</b> — stop starting new polls.
     *   <li><b>Fetch pool</b> — let any in-flight fetch finish enqueuing.
     *   <li><b>Workers</b> — tell them to stop, but they drain their shard first.
     *   <li>Only then does the caller close the Kafka publisher and flush.
     * </ol>
     *
     * Reversing any two of these drops positions: stop workers first and the queue is abandoned;
     * flush Kafka first and the last batch of records is produced after the flush.
     */
    @Override
    public void close() {
        // Reachable from both the shutdown hook and ordinary control flow; see
        // KafkaPositionPublisher.close() for why that matters.
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        log.info("Shutting down ingest. Queue holds {} items.", queue.depth());

        scheduler.shutdown();
        awaitOrKill(scheduler, "scheduler", 5);

        fetchExecutor.shutdown();
        awaitOrKill(fetchExecutor, "fetch pool", 10);

        workers.forEach(PositionWorker::stop);
        workerExecutor.shutdown();
        awaitOrKill(workerExecutor, "workers", 15);

        if (!queue.isEmpty()) {
            log.warn("{} positions were still queued at shutdown and have been dropped.", queue.depth());
        }
        reportMetrics();
        log.info("Final state: {}", store);
    }

    private static void awaitOrKill(ExecutorService executor, String what, int seconds) {
        try {
            if (!executor.awaitTermination(seconds, TimeUnit.SECONDS)) {
                log.warn("{} did not finish in {}s; interrupting.", what, seconds);
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    public VehicleStore store() {
        return store;
    }

    public FeedPoller poller() {
        return poller;
    }

    public ShardedPositionQueue queue() {
        return queue;
    }

    public IngestMetrics metrics() {
        return metrics;
    }
}
