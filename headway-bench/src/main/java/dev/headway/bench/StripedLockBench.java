package dev.headway.bench;

import com.google.common.util.concurrent.Striped;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
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
 * Updating two related values atomically, per key.
 *
 * <p>{@code AlertTracker} has this shape: an episode lives in one structure and moves to another
 * when it closes. Today those are deliberately separate steps - {@code remove} then archive - which
 * cannot deadlock precisely because they are not one transaction. If they ever had to be atomic,
 * the question becomes how to lock per route instead of globally.
 *
 * <ul>
 *   <li><b>One lock.</b> Correct and trivially auditable. Every route waits for every other route.
 *   <li><b>{@link Striped}.</b> A fixed number of locks, key hashed onto one. Two routes contend
 *       only when they collide on a stripe. Same idea as step 4's sharded queue and as
 *       ConcurrentHashMap's own bin locks. The stripe count is fixed, so unlike a lock-per-key map
 *       memory does not grow with the key space - which matters when keys are user-supplied.
 *   <li><b>One composite value.</b> Sidestep locking: put both fields in one immutable record and
 *       swap it with {@code compute}. Atomic per key with no lock object at all.
 * </ul>
 *
 * <p>All three read and write through a {@link ConcurrentHashMap}. Striped locks over a plain
 * {@code HashMap} would be a data race even though every access is "inside a lock", because two
 * threads holding <em>different</em> stripes touch the same map with no ordering between them.
 * The stripes provide atomicity of the compound operation; they do not make the container safe.
 * Getting that backwards is the classic way to misuse this class.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(2)
@Warmup(iterations = 5, time = 2)
@Measurement(iterations = 5, time = 2)
@Threads(8)
@State(Scope.Benchmark)
public class StripedLockBench {

    @Param({"200"})
    public int keys;

    /**
     * Stripe counts are folded into the implementation name rather than being a separate parameter,
     * so the result table has one row per thing actually being compared instead of nine rows where
     * six repeat.
     *
     * <p>{@code striped8} is deliberately a bad choice - one stripe per thread, so collisions are
     * routine. It is here because "use striping" is not advice; "use more stripes than threads" is.
     */
    @Param({"singleLock", "striped8", "striped64", "striped512", "compositeCompute"})
    public String impl;

    private interface Update {
        int apply(String key, long seq);
    }

    private Update update;
    private String[] ids;
    private int mask;

    private record Episode(int count, long lastSeen) {}

    @State(Scope.Thread)
    public static class Cursor {
        long seq;

        @Setup(Level.Iteration)
        public void init() {
            seq = Thread.currentThread().threadId() * 7_919L;
        }
    }

    @Setup(Level.Iteration)
    public void setUp() {
        int capacity = Integer.highestOneBit(Math.max(2, keys) - 1) * 2;
        ids = new String[capacity];
        for (int i = 0; i < capacity; i++) {
            ids[i] = "route-" + i;
        }
        mask = capacity - 1;

        Map<String, Integer> counts = new ConcurrentHashMap<>();
        Map<String, Long> lastSeen = new ConcurrentHashMap<>();
        ConcurrentHashMap<String, Episode> composite = new ConcurrentHashMap<>();
        for (String id : ids) {
            counts.put(id, 0);
            lastSeen.put(id, 0L);
            composite.put(id, new Episode(0, 0));
        }

        update = switch (impl) {
            case "singleLock" -> {
                Lock lock = new ReentrantLock();
                yield (key, seq) -> {
                    lock.lock();
                    try {
                        int next = counts.get(key) + 1;
                        counts.put(key, next);
                        lastSeen.put(key, seq);
                        return next;
                    } finally {
                        lock.unlock();
                    }
                };
            }
            case "striped8", "striped64", "striped512" -> {
                Striped<Lock> striped = Striped.lock(Integer.parseInt(impl.substring("striped".length())));
                yield (key, seq) -> {
                    Lock lock = striped.get(key);
                    lock.lock();
                    try {
                        int next = counts.get(key) + 1;
                        counts.put(key, next);
                        lastSeen.put(key, seq);
                        return next;
                    } finally {
                        lock.unlock();
                    }
                };
            }
            case "compositeCompute" -> (key, seq) ->
                    composite.compute(key, (k, existing) -> new Episode(existing.count() + 1, seq))
                            .count();
            default -> throw new IllegalArgumentException(impl);
        };
    }

    @Benchmark
    public int compoundUpdate(Cursor cursor) {
        long seq = cursor.seq++;
        return update.apply(ids[(int) (seq & mask)], seq);
    }
}
