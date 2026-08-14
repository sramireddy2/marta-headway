package dev.headway.ingest;

import dev.headway.common.VehiclePosition;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Owns the background threads: polls the feed on a timer and sweeps out dead vehicles.
 *
 * <p>Implements {@link AutoCloseable} so it can be used in try-with-resources and so shutdown is
 * impossible to forget.
 */
public final class IngestService implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(IngestService.class);

    /** All the knobs in one place, with defaults that are polite to MARTA. */
    public record Config(
            Duration pollInterval,
            double maxRequestsPerSecond,
            Duration evictAfter,
            Duration evictionSweepInterval) {

        public static Config defaults() {
            return new Config(
                    Duration.ofSeconds(15),   // MARTA republishes roughly this often
                    1.0 / 15.0,               // ceiling: one request per 15 seconds
                    Duration.ofMinutes(10),   // a bus silent for 10 min has finished its run
                    Duration.ofMinutes(1));
        }
    }

    private final VehicleStore store;
    private final FeedPoller poller;
    private final Config config;
    private final ScheduledExecutorService scheduler;

    public IngestService(VehicleFeed feed, Config config,
                         Consumer<List<VehiclePosition>> downstream) {
        this.config = config;
        this.store = new VehicleStore();
        this.poller = new FeedPoller(feed, store, config.maxRequestsPerSecond(), downstream);

        // Two threads: one polls, one evicts. Keeping eviction off the polling thread means a slow
        // sweep can never delay a poll.
        //
        // These are named. Unnamed executor threads show up in stack traces and profilers as
        // "pool-1-thread-1", which tells you nothing at 2am. Naming threads costs one class and
        // pays for itself the first time you read a thread dump.
        this.scheduler = Executors.newScheduledThreadPool(2, namedThreadFactory("headway-ingest"));
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

    /** Starts polling. Returns immediately; work happens on the background threads. */
    public void start() {
        log.info("Starting ingest: poll every {}s, ceiling {} req/s, evict after {}min",
                config.pollInterval().toSeconds(),
                "%.4f".formatted(config.maxRequestsPerSecond()),
                config.evictAfter().toMinutes());

        // scheduleWithFixedDelay, NOT scheduleAtFixedRate.
        //
        // atFixedRate starts a new run every N seconds measured from the previous *start*. If the
        // feed hangs for 40s on a 15s schedule, the missed runs queue up and then fire
        // back-to-back the instant it recovers — hammering a server that is already struggling.
        //
        // withFixedDelay waits N seconds after the previous run *finishes*. Slow responses simply
        // slow the loop down, which is exactly the behaviour you want when talking to something
        // you do not control.
        scheduler.scheduleWithFixedDelay(
                poller, 0, config.pollInterval().toMillis(), TimeUnit.MILLISECONDS);

        scheduler.scheduleWithFixedDelay(
                this::sweep,
                config.evictionSweepInterval().toMillis(),
                config.evictionSweepInterval().toMillis(),
                TimeUnit.MILLISECONDS);
    }

    private void sweep() {
        try {
            int removed = store.evictStale(config.evictAfter());
            if (removed > 0) {
                log.info("Evicted {} stale vehicles; {} remain", removed, store.size());
            }
        } catch (Throwable t) {
            // Same reason as FeedPoller.run(): a throw here would cancel the sweep forever.
            log.error("Eviction sweep failed. Continuing.", t);
        }
    }

    /**
     * Stops the background threads, giving in-flight work a chance to finish.
     *
     * <p>The two-phase dance below is the standard idiom and worth memorising:
     *
     * <ol>
     *   <li>{@code shutdown()} — stop accepting new work, let running tasks finish.
     *   <li>{@code awaitTermination(...)} — wait, but not forever.
     *   <li>{@code shutdownNow()} — interrupt whatever is still stuck.
     * </ol>
     *
     * Calling only {@code shutdown()} can hang forever on a wedged socket. Calling only
     * {@code shutdownNow()} kills work that was two milliseconds from completing.
     */
    @Override
    public void close() {
        log.info("Shutting down ingest. Final state: {}", store);
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                log.warn("Tasks did not finish in 5s; interrupting them.");
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    public VehicleStore store() {
        return store;
    }

    public FeedPoller poller() {
        return poller;
    }
}
