package dev.headway.bench;

import dev.headway.bench.Stores.PositionStore;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

/**
 * How much does making the idempotency check atomic actually cost?
 *
 * <p>The project asserts, in several places, that {@code ConcurrentHashMap.compute} is the right
 * way to do read-decide-write on a shared map, and that a single lock over the whole map would
 * serialise threads that never touch the same key. This measures both claims, and the price of
 * the racy version that is easier to write.
 *
 * <p>Two paths are measured separately because the live feed exercises both heavily. A live run
 * processed 3,090 pings and found 1,565 of them stale - the feed republishes every bus on every
 * poll whether or not it has moved, so about half of all traffic is a re-send that has to be
 * recognised and discarded. That is the {@code rejectStale} path, and it is not the one people
 * optimise for.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(2)
@Warmup(iterations = 5, time = 2)
@Measurement(iterations = 5, time = 2)
@State(Scope.Benchmark)
public class StoreUpdateBench {

    /**
     * Roughly the size of MARTA's active fleet. It matters: with few keys, every thread collides on
     * the same ConcurrentHashMap bins and the striping advantage disappears - which is itself worth
     * knowing, so it is a parameter rather than a constant.
     */
    @Param({"200"})
    public int fleetSize;

    @Param({"compute", "fastPathCompute", "racy", "synchronized", "readWriteLock", "stampedLock"})
    public String impl;

    private PositionStore store;
    private String[] ids;
    private int mask;

    @State(Scope.Thread)
    public static class Cursor {
        long seq;
        /** Distinct per thread so threads do not march in lockstep over the same keys. */
        @Setup(Level.Iteration)
        public void init() {
            seq = Thread.currentThread().threadId() * 1_000_003L;
        }
    }

    @Setup(Level.Iteration)
    public void setUp() {
        store = switch (impl) {
            case "compute" -> new Stores.ComputeStore();
            case "fastPathCompute" -> new Stores.FastPathComputeStore();
            case "racy" -> new Stores.RacyStore();
            case "synchronized" -> new Stores.SynchronizedStore();
            case "readWriteLock" -> new Stores.ReadWriteLockStore();
            case "stampedLock" -> new Stores.StampedLockStore();
            default -> throw new IllegalArgumentException(impl);
        };

        int capacity = Integer.highestOneBit(Math.max(2, fleetSize) - 1) * 2;
        ids = new String[capacity];
        for (int i = 0; i < capacity; i++) {
            ids[i] = "bus-" + i;
        }
        mask = capacity - 1;

        // Seed with a very high timestamp so the rejectStale path really does reject.
        for (String id : ids) {
            store.accept(id, Long.MAX_VALUE / 2);
        }
    }

    /**
     * The write path: every reading is newer, so every one is applied.
     *
     * <p>Timestamps come from an ever-increasing counter rather than a precomputed array, so there
     * is no wraparound quietly turning writes into rejections partway through an iteration.
     */
    @Benchmark
    @Threads(8)
    public boolean applyNewer_8threads(Cursor cursor) {
        long seq = cursor.seq++;
        return store.accept(ids[(int) (seq & mask)], Long.MAX_VALUE / 2 + seq + 1);
    }

    /** The common path in production: a re-sent reading that has to be recognised and dropped. */
    @Benchmark
    @Threads(8)
    public boolean rejectStale_8threads(Cursor cursor) {
        long seq = cursor.seq++;
        return store.accept(ids[(int) (seq & mask)], 1_000L);
    }

    /** Uncontended, to separate "this lock is slow" from "this lock is contended". */
    @Benchmark
    @Threads(1)
    public boolean applyNewer_1thread(Cursor cursor) {
        long seq = cursor.seq++;
        return store.accept(ids[(int) (seq & mask)], Long.MAX_VALUE / 2 + seq + 1);
    }
}
