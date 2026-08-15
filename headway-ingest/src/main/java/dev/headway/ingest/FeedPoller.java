package dev.headway.ingest;

import com.google.common.util.concurrent.RateLimiter;
import dev.headway.common.VehiclePosition;
import dev.headway.ingest.pipeline.IngestMetrics;
import dev.headway.ingest.pipeline.ShardedPositionQueue;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One poll of one feed: rate-limit, fetch, decode, hand every position to the queue.
 *
 * <p>Since step 4 the poller does <em>not</em> touch the store or Kafka. Its only job is to get
 * data out of the network and into the queue; workers do everything after that. That split is what
 * makes backpressure possible — there is now a boundary where "producers are outrunning consumers"
 * is a thing that can be observed and acted on, rather than one long call stack.
 *
 * <h2>The footgun this class exists to avoid</h2>
 *
 * {@link java.util.concurrent.ScheduledExecutorService#scheduleWithFixedDelay} has a behaviour that
 * catches almost everyone once: <b>if your task throws, the schedule is cancelled — permanently and
 * silently.</b> No log line, no exception anywhere, no crash. Your app keeps running and simply
 * never polls again. People discover it hours later when the dashboard has flatlined.
 *
 * <p>So {@link #run()} catches {@link Throwable} and swallows it after logging. One bad HTTP
 * response must not end the service. Every scheduled task you ever write needs this wrapper.
 */
public final class FeedPoller implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(FeedPoller.class);

    private final String name;
    private final VehicleFeed feed;
    private final ShardedPositionQueue queue;
    private final RateLimiter rateLimiter;
    private final IngestMetrics metrics;

    private final LongAdder pollsAttempted = new LongAdder();
    private final LongAdder pollsFailed = new LongAdder();
    private final AtomicInteger consecutiveFailures = new AtomicInteger();

    /**
     * @param maxRequestsPerSecond politeness ceiling for this feed. MARTA publishes every ~15s, so
     *     anything above {@code 1/15} is wasted bandwidth for them and for you.
     */
    public FeedPoller(String name, VehicleFeed feed, ShardedPositionQueue queue,
                      double maxRequestsPerSecond, IngestMetrics metrics) {
        this.name = name;
        this.feed = feed;
        this.queue = queue;
        this.metrics = metrics;

        // A token bucket. Permits accrue at maxRequestsPerSecond; acquire() takes one, blocking
        // the calling thread if none is available yet.
        //
        // The scheduler already spaces polls 15s apart, so why also have this? Because the
        // scheduler controls *cadence* and the limiter enforces a *ceiling*. The moment you add a
        // second feed, a manual refresh endpoint, or a retry loop, cadence stops being a
        // guarantee. The limiter still holds. Getting IP-banned from a public feed is a real and
        // embarrassing way to break your own demo.
        this.rateLimiter = RateLimiter.create(maxRequestsPerSecond);
    }

    /** Scheduler entry point. Never throws — see the class javadoc. */
    @Override
    public void run() {
        try {
            pollOnce();
        } catch (InterruptedException e) {
            // Someone asked this thread to stop. Restore the flag we just cleared by catching, so
            // code further up the stack can still see the interrupt. Never swallow it outright.
            Thread.currentThread().interrupt();
            log.info("Poller {} interrupted; stopping.", name);
        } catch (Throwable t) {
            // Deliberately Throwable, not Exception. An OutOfMemoryError or a NoClassDefFoundError
            // would otherwise silently kill the schedule too.
            log.error("Poll {} failed ({} in a row). Continuing.", name, consecutiveFailures.get(), t);
        }
    }

    /** Does the actual work. Returns what happened. */
    public Result pollOnce() throws IOException, InterruptedException {
        pollsAttempted.increment();

        double waitedSeconds = rateLimiter.acquire(); // blocks until a permit is free
        if (waitedSeconds > 0.01) {
            log.debug("Rate limiter held {} for {}s", name, "%.2f".formatted(waitedSeconds));
        }

        long startNanos = System.nanoTime();
        List<VehiclePosition> positions;
        try {
            positions = feed.fetchVehiclePositions();
        } catch (IOException | InterruptedException e) {
            pollsFailed.increment();
            consecutiveFailures.incrementAndGet();
            metrics.recordPollFailure();
            throw e;
        }
        consecutiveFailures.set(0);
        metrics.recordFetched(positions.size());

        // Hand off to the queue. Any of these puts can block if a shard is full; that is the
        // pipeline telling us to slow down, and blocking here is the correct response.
        long blockedNanos = 0;
        for (VehiclePosition position : positions) {
            blockedNanos += queue.put(position);
        }

        long totalNanos = System.nanoTime() - startNanos;
        metrics.pollDuration().record(totalNanos, TimeUnit.NANOSECONDS);

        Result result = new Result(positions.size(), totalNanos / 1_000_000, blockedNanos / 1_000_000);

        if (result.blockedMillis() > 0) {
            log.info("poll {}: {} positions enqueued in {}ms ({}ms BLOCKED on a full queue) | queue {}/{}",
                    name, result.received(), result.durationMillis(), result.blockedMillis(),
                    queue.depth(), queue.totalCapacity());
        } else {
            log.debug("poll {}: {} positions enqueued in {}ms | queue {}/{}",
                    name, result.received(), result.durationMillis(),
                    queue.depth(), queue.totalCapacity());
        }
        return result;
    }

    /** Outcome of one poll. */
    public record Result(int received, long durationMillis, long blockedMillis) {}

    public String name() {
        return name;
    }

    public long pollsAttemptedTotal() {
        return pollsAttempted.sum();
    }

    public long pollsFailedTotal() {
        return pollsFailed.sum();
    }

    public int consecutiveFailures() {
        return consecutiveFailures.get();
    }
}
