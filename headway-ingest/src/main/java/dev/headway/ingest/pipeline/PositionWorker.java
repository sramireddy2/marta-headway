package dev.headway.ingest.pipeline;

import dev.headway.common.VehiclePosition;
import dev.headway.ingest.VehicleStore;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Drains one shard: take a position, update the store, publish it. Forever, until told to stop.
 *
 * <p>Exactly one worker exists per shard, which is what preserves per-route ordering — see
 * {@link ShardedPositionQueue}.
 *
 * <p>This is also where step 2's concurrency work stops being theoretical. Four workers now call
 * {@link VehicleStore#apply} at the same time on genuinely different threads. The
 * {@code ConcurrentHashMap.compute} that looked like defensive over-engineering when one thread
 * used it is now load-bearing.
 */
public final class PositionWorker implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(PositionWorker.class);

    /**
     * How long a worker waits on an empty shard before looking up.
     *
     * <p>Not a poll interval — the queue wakes the thread the instant an item arrives. It is only
     * the maximum delay between being asked to stop and noticing, so shutdown is bounded at 200ms
     * rather than blocking forever on a queue nothing will ever fill again.
     */
    private static final long IDLE_WAIT_MILLIS = 200;

    private final int shard;
    private final ShardedPositionQueue queue;
    private final VehicleStore store;
    private final Consumer<VehiclePosition> publisher;
    private final IngestMetrics metrics;

    private volatile boolean running = true;

    public PositionWorker(int shard, ShardedPositionQueue queue, VehicleStore store,
                          Consumer<VehiclePosition> publisher, IngestMetrics metrics) {
        this.shard = shard;
        this.queue = queue;
        this.store = store;
        this.publisher = publisher;
        this.metrics = metrics;
    }

    @Override
    public void run() {
        log.debug("Worker {} started", shard);

        // `running || not empty` rather than just `running`: on shutdown the worker finishes what
        // is already queued before exiting. Stopping with items still in the queue would silently
        // discard positions that were accepted from the feed.
        while (running || queue.depthOf(shard) > 0) {
            try {
                VehiclePosition position = queue.poll(shard, IDLE_WAIT_MILLIS, TimeUnit.MILLISECONDS);
                if (position != null) {
                    handle(position);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.debug("Worker {} interrupted", shard);
                return;
            } catch (Throwable t) {
                // One malformed position must not kill the worker and silently take a quarter of
                // the routes offline. Same reasoning as FeedPoller.run().
                log.error("Worker {} failed on one position. Continuing.", shard, t);
            }
        }
        log.debug("Worker {} finished, {} left in shard", shard, queue.depthOf(shard));
    }

    private void handle(VehiclePosition position) {
        long start = System.nanoTime();

        VehicleStore.Outcome outcome = store.apply(position);
        switch (outcome) {
            case STALE -> metrics.recordStale();
            case REJECTED -> metrics.recordRejected();
            case NEW, UPDATED -> { }
        }

        // Publish everything the feed gave us, including duplicates the store ignored. Kafka is
        // the durable record of what the agency actually said; the store is our derived view of
        // current state. Filtering the log to match the view would make the topic unreplayable.
        publisher.accept(position);

        metrics.processDuration().record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        metrics.recordProcessed();
    }

    /** Asks the worker to finish the current shard contents and exit. */
    public void stop() {
        running = false;
    }

    public int shard() {
        return shard;
    }
}
