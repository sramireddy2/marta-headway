package dev.headway.ingest.pipeline;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.function.Supplier;

/**
 * Every number the pipeline reports about itself.
 *
 * <p>Micrometer is a vendor-neutral metrics facade — the SLF4J of instrumentation. Your code says
 * "increment this counter" and a <em>registry</em> decides where that lands. Here it is a
 * {@link SimpleMeterRegistry}, which just keeps values in memory so we can log them. In step 9,
 * swapping in Spring Boot's Prometheus registry exposes exactly these meters over HTTP with no
 * change to any of the call sites below.
 *
 * <h2>Why bother instrumenting a project nobody is paging you about</h2>
 *
 * Because backpressure is invisible without it. A pipeline that is coping and a pipeline that is
 * one slow consumer away from stalling look identical from the outside — same log lines, same
 * throughput, right up until they are not. The difference lives entirely in two numbers: how full
 * the queue is, and how long producers are spending blocked. Those are the two meters worth
 * understanding here; the rest are context.
 *
 * <h2>The three meter types</h2>
 *
 * <ul>
 *   <li><b>Counter</b> — only goes up. Totals: positions enqueued, records published.
 *   <li><b>Gauge</b> — a value sampled on read, up and down. Queue depth, store size. Micrometer
 *       holds a <em>weak</em> reference to the source, so the object being measured must be kept
 *       alive by something else; a gauge whose target has been collected silently reports NaN.
 *   <li><b>Timer</b> — count plus total duration plus a distribution. How long a poll takes, and
 *       how long producers block.
 * </ul>
 */
public final class IngestMetrics {

    private final MeterRegistry registry;

    private final Counter positionsFetched;
    private final Counter positionsEnqueued;
    private final Counter positionsProcessed;
    private final Counter positionsStale;
    private final Counter positionsRejected;
    private final Counter pollFailures;
    private final Counter kafkaSent;
    private final Counter kafkaFailed;

    private final Timer pollDuration;
    private final Timer enqueueWait;
    private final Timer processDuration;

    public IngestMetrics() {
        this(new SimpleMeterRegistry());
    }

    public IngestMetrics(MeterRegistry registry) {
        this.registry = registry;

        this.positionsFetched = Counter.builder("headway.positions.fetched")
                .description("Positions decoded from the feed").register(registry);
        this.positionsEnqueued = Counter.builder("headway.positions.enqueued")
                .description("Positions handed to the queue").register(registry);
        this.positionsProcessed = Counter.builder("headway.positions.processed")
                .description("Positions taken off the queue and handled").register(registry);
        this.positionsStale = Counter.builder("headway.positions.stale")
                .description("Duplicate or out-of-order readings dropped").register(registry);
        this.positionsRejected = Counter.builder("headway.positions.rejected")
                .description("Readings refused as too old or future-dated").register(registry);
        this.pollFailures = Counter.builder("headway.poll.failures")
                .description("Feed fetches that threw").register(registry);
        this.kafkaSent = Counter.builder("headway.kafka.sent")
                .description("Records acknowledged by the broker").register(registry);
        this.kafkaFailed = Counter.builder("headway.kafka.failed")
                .description("Records the broker rejected").register(registry);

        this.pollDuration = Timer.builder("headway.poll.duration")
                .description("Fetch and decode one feed").register(registry);

        // THE backpressure meter. If this total is climbing, producers are being held back by
        // consumers, which is the system working as designed — but it is also the early warning
        // that you are at capacity. Zero here means the queue has never been full.
        this.enqueueWait = Timer.builder("headway.enqueue.wait")
                .description("Time producers spent blocked on a full queue").register(registry);

        this.processDuration = Timer.builder("headway.process.duration")
                .description("Handle one position: store update plus publish").register(registry);
    }

    /**
     * Registers a gauge backed by a supplier.
     *
     * <p>Note the {@code this} in {@code Gauge.builder(name, this, ...)}: Micrometer keeps only a
     * weak reference to a gauge's source object. Anchoring it to this long-lived metrics instance
     * rather than to a lambda's captured state is what stops the gauge quietly turning into NaN
     * after a garbage collection — a genuinely baffling bug the first time you hit it.
     */
    public void gauge(String name, String description, Supplier<Number> value) {
        Gauge.builder(name, this, unused -> value.get().doubleValue())
                .description(description)
                .strongReference(true)
                .register(registry);
    }

    public MeterRegistry registry() {
        return registry;
    }

    public void recordFetched(int count) {
        positionsFetched.increment(count);
    }

    public void recordEnqueued() {
        positionsEnqueued.increment();
    }

    public void recordProcessed() {
        positionsProcessed.increment();
    }

    public void recordStale() {
        positionsStale.increment();
    }

    public void recordRejected() {
        positionsRejected.increment();
    }

    public void recordPollFailure() {
        pollFailures.increment();
    }

    public void recordKafkaSent() {
        kafkaSent.increment();
    }

    public void recordKafkaFailed() {
        kafkaFailed.increment();
    }

    public Timer pollDuration() {
        return pollDuration;
    }

    public Timer enqueueWait() {
        return enqueueWait;
    }

    public Timer processDuration() {
        return processDuration;
    }

    public double totalEnqueueWaitMillis() {
        return enqueueWait.totalTime(java.util.concurrent.TimeUnit.MILLISECONDS);
    }

    public long fetchedTotal() {
        return (long) positionsFetched.count();
    }

    public long enqueuedTotal() {
        return (long) positionsEnqueued.count();
    }

    public long processedTotal() {
        return (long) positionsProcessed.count();
    }

    public long staleTotal() {
        return (long) positionsStale.count();
    }

    public long rejectedTotal() {
        return (long) positionsRejected.count();
    }

    public long kafkaSentTotal() {
        return (long) kafkaSent.count();
    }

    public long kafkaFailedTotal() {
        return (long) kafkaFailed.count();
    }
}
