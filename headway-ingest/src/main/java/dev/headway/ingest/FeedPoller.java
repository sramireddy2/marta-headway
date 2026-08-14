package dev.headway.ingest;

import com.google.common.util.concurrent.RateLimiter;
import dev.headway.common.VehiclePosition;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One poll of one feed: rate-limit, fetch, decode, apply to the store, hand off downstream.
 *
 * <p>This class is a {@link Runnable} so a scheduler can drive it, but the real work lives in
 * {@link #pollOnce()}, which returns a result and is allowed to throw. Tests call {@code pollOnce},
 * the scheduler calls {@code run}.
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

    private final VehicleFeed feed;
    private final VehicleStore store;
    private final RateLimiter rateLimiter;
    private final Consumer<List<VehiclePosition>> downstream;

    private final LongAdder pollsAttempted = new LongAdder();
    private final LongAdder pollsFailed = new LongAdder();
    private final AtomicInteger consecutiveFailures = new AtomicInteger();

    /**
     * @param maxRequestsPerSecond politeness ceiling for this feed. MARTA publishes every ~15s, so
     *     anything above {@code 1/15} is wasted bandwidth for them and for you.
     * @param downstream where decoded positions go next. Today: a logger. In step 3: Kafka.
     */
    public FeedPoller(
            VehicleFeed feed,
            VehicleStore store,
            double maxRequestsPerSecond,
            Consumer<List<VehiclePosition>> downstream) {
        this.feed = feed;
        this.store = store;
        this.downstream = downstream;

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
            log.info("Poller interrupted; stopping.");
        } catch (Throwable t) {
            // Deliberately Throwable, not Exception. An OutOfMemoryError or a NoClassDefFoundError
            // would otherwise silently kill the schedule too.
            log.error("Poll failed ({} in a row). Continuing.", consecutiveFailures.get(), t);
        }
    }

    /** Does the actual work. Returns what changed. */
    public Result pollOnce() throws IOException, InterruptedException {
        pollsAttempted.increment();

        double waitedSeconds = rateLimiter.acquire(); // blocks until a permit is free
        if (waitedSeconds > 0.01) {
            log.debug("Rate limiter held the poll for {}s", "%.2f".formatted(waitedSeconds));
        }

        long startNanos = System.nanoTime();
        List<VehiclePosition> positions;
        try {
            positions = feed.fetchVehiclePositions();
        } catch (IOException | InterruptedException e) {
            pollsFailed.increment();
            consecutiveFailures.incrementAndGet();
            throw e;
        }
        consecutiveFailures.set(0);

        VehicleStore.Stats stats = store.applyAll(positions);
        downstream.accept(positions);

        long millis = (System.nanoTime() - startNanos) / 1_000_000;
        Result result = new Result(positions.size(), stats, millis);

        log.info("poll: {} received | {} new, {} updated, {} stale, {} rejected"
                        + " | store holds {} vehicles on {} routes | {}ms",
                result.received(), stats.created(), stats.updated(), stats.stale(), stats.rejected(),
                store.size(), store.routeCount(), millis);

        return result;
    }

    /** Outcome of one poll. */
    public record Result(int received, VehicleStore.Stats stats, long durationMillis) {}

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
