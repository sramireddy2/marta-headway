package dev.headway.api.web;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;

/**
 * Runs a slow task on one background thread, keeping only the newest input when work piles up.
 *
 * <h2>Queues are the wrong tool for snapshots</h2>
 *
 * The obvious way to hand work to a background thread is a queue. That is right when every item
 * must be processed — a payment, a log line, a Kafka record. It is wrong here. Each submission is a
 * complete picture of the current state, so if three arrive while the sender is busy, delivering
 * all three means the browser draws two frames that were already obsolete before they were sent.
 * The work is real, the result is not.
 *
 * <p>Worse, a bounded queue that fills has to choose between blocking the caller — coupling the
 * browser's speed to the sweep timer — and dropping the <em>newest</em> item, which is the only one
 * that mattered. This class sidesteps the choice: {@link #submit} overwrites the pending value
 * instead of appending to anything. Old snapshots are not queued and then discarded; they are never
 * queued at all. The same idea drives market data fan-out, where a stale price is worse than no
 * price.
 *
 * <h2>How the handoff stays correct without a lock</h2>
 *
 * A single {@link AtomicReference} does the whole job.
 *
 * <ul>
 *   <li>{@code submit} does {@code getAndSet(value)}. Getting null back means no drain is pending,
 *       so it schedules one. Getting non-null back means a drain is already scheduled and will pick
 *       up this newer value when it runs.
 *   <li>{@code drain} does {@code getAndSet(null)} and processes what it took.
 * </ul>
 *
 * <p>The interesting case is a submit landing between the drain's {@code getAndSet(null)} and the
 * end of its work. That submit sees null, so it schedules a second drain — correct, because the
 * value it just stored would otherwise never be sent. The executor is single-threaded, so the two
 * drains are serialised and the sink is never called concurrently with itself.
 */
public final class Conflator<T> implements AutoCloseable {

    private final AtomicReference<T> pending = new AtomicReference<>();
    private final ExecutorService worker;
    private final Consumer<T> sink;

    private final LongAdder submitted = new LongAdder();
    private final LongAdder delivered = new LongAdder();

    public Conflator(String threadName, Consumer<T> sink) {
        this.sink = sink;
        this.worker = Executors.newSingleThreadExecutor(runnable -> {
            Thread thread = new Thread(runnable, threadName);
            thread.setDaemon(true);
            return thread;
        });
    }

    /** Offers a value. Never blocks, never throws, and supersedes anything not yet delivered. */
    public void submit(T value) {
        if (value == null) {
            return;
        }
        submitted.increment();
        if (pending.getAndSet(value) == null) {
            try {
                worker.execute(this::drain);
            } catch (RejectedExecutionException shuttingDown) {
                pending.set(null);
            }
        }
    }

    private void drain() {
        T value = pending.getAndSet(null);
        if (value == null) {
            return;
        }
        try {
            sink.accept(value);
            delivered.increment();
        } catch (RuntimeException e) {
            // Swallowed on purpose. An executor task that throws is not retried and, for a
            // scheduled executor, would silently cancel all future work. The sink is responsible
            // for its own logging; what matters here is that the next drain still happens.
            delivered.increment();
        }
    }

    /** How many submissions were superseded before they could be sent. */
    public long coalescedCount() {
        return Math.max(0, submitted.sum() - delivered.sum());
    }

    public long submittedCount() {
        return submitted.sum();
    }

    public long deliveredCount() {
        return delivered.sum();
    }

    @Override
    public void close() {
        worker.shutdownNow();
        try {
            worker.awaitTermination(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
