package dev.headway.bench;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.StampedLock;

/**
 * Five ways to write "keep this reading only if it is newer than the one I already have".
 *
 * <p>That is {@code VehicleStore.accept} and {@code LiveHeadwayState.accept} with the payload taken
 * out. The payload is deliberately reduced to a timestamp: every implementation below would pay the
 * identical cost to carry a {@code VehiclePosition} instead, so including one would add a constant
 * to all five results and shrink the differences the benchmark exists to show.
 *
 * <p>The operation matters because it is <em>read-decide-write</em>. Getting it wrong does not
 * corrupt the map; it silently loses an update, which is invisible until you go looking.
 */
final class Stores {

    private Stores() {}

    interface PositionStore {

        /** @return true if the reading was applied */
        boolean accept(String vehicleId, long timestampMillis);

        Long get(String vehicleId);
    }

    /**
     * What the project actually uses.
     *
     * <p>{@code compute} runs the remapping function while holding the lock on that key's bin, so
     * read, decide and write are one indivisible step. Crucially the lock is per-bin, not per-map:
     * threads touching different routes do not contend at all.
     */
    static final class ComputeStore implements PositionStore {
        private final ConcurrentHashMap<String, Long> map = new ConcurrentHashMap<>();

        @Override
        public boolean accept(String vehicleId, long timestampMillis) {
            boolean[] applied = {false};
            map.compute(vehicleId, (key, existing) -> {
                if (existing != null && existing >= timestampMillis) {
                    return existing;
                }
                applied[0] = true;
                return timestampMillis;
            });
            return applied[0];
        }

        @Override
        public Long get(String vehicleId) {
            return map.get(vehicleId);
        }
    }

    /**
     * The same thing written the obvious way, and wrong.
     *
     * <p>Between the {@code get} and the {@code put} another thread can complete an entire update.
     * Both threads read the same old value, both decide they are newer, and whichever writes second
     * wins - which may be the older of the two. It is included here because it is <b>faster</b>,
     * and knowing the price of the correct version is the only way to argue for it honestly. An
     * earlier step of this project reproduced the loss empirically: 5 runs in 40 lost data.
     */
    static final class RacyStore implements PositionStore {
        private final ConcurrentHashMap<String, Long> map = new ConcurrentHashMap<>();

        @Override
        public boolean accept(String vehicleId, long timestampMillis) {
            Long existing = map.get(vehicleId);
            if (existing != null && existing >= timestampMillis) {
                return false;
            }
            map.put(vehicleId, timestampMillis);          // <-- the window
            return true;
        }

        @Override
        public Long get(String vehicleId) {
            return map.get(vehicleId);
        }
    }

    /**
     * {@link ComputeStore} with a read in front of it — correct, and much cheaper on the path that
     * dominates in production.
     *
     * <p>The first benchmark run showed the racy version beating {@code compute} by more than ten
     * times on stale re-sends, and the reason is not that the race is fast: it is that
     * {@code compute} takes the bin lock <em>even to decide to do nothing</em>. On this feed that
     * is the single most common outcome: a live run processed 3,090 pings of which 1,565 were
     * stale, because MARTA republishes every bus on every poll whether or not it has moved.
     *
     * <p>So: do a plain lock-free {@code get} first. If it proves the incoming reading is not newer,
     * return immediately — no lock. Otherwise fall through to {@code compute}, which re-checks under
     * the bin lock and is the thing that actually makes the decision.
     *
     * <p><b>Why this is not the check-then-act race again.</b> The fast path only ever skips work.
     * A stale value read here can cause a needless {@code compute} call, never a lost update,
     * because the authoritative comparison still happens inside the lock. The racy version's bug is
     * that its decision is final; this one's is only a hint. That difference is the whole of it, and
     * it is the reason the guard is written as "provably not newer" rather than "newer".
     */
    static final class FastPathComputeStore implements PositionStore {
        private final ConcurrentHashMap<String, Long> map = new ConcurrentHashMap<>();

        @Override
        public boolean accept(String vehicleId, long timestampMillis) {
            Long existing = map.get(vehicleId);
            if (existing != null && existing >= timestampMillis) {
                return false;                     // provably not newer; a lock would change nothing
            }
            boolean[] applied = {false};
            map.compute(vehicleId, (key, current) -> {
                if (current != null && current >= timestampMillis) {
                    return current;
                }
                applied[0] = true;
                return timestampMillis;
            });
            return applied[0];
        }

        @Override
        public Long get(String vehicleId) {
            return map.get(vehicleId);
        }
    }

    /** One lock for the whole map: correct, and the baseline everything else has to beat. */
    static final class SynchronizedStore implements PositionStore {
        private final Map<String, Long> map = new HashMap<>();

        @Override
        public synchronized boolean accept(String vehicleId, long timestampMillis) {
            Long existing = map.get(vehicleId);
            if (existing != null && existing >= timestampMillis) {
                return false;
            }
            map.put(vehicleId, timestampMillis);
            return true;
        }

        @Override
        public synchronized Long get(String vehicleId) {
            return map.get(vehicleId);
        }
    }

    /**
     * A read/write lock, which sounds ideal for a read-mostly workload and usually is not.
     *
     * <p>The catch is that acquiring even a <em>read</em> lock is a write to the lock's own state
     * word. Every reader on every core hammers the same cache line, so readers contend with each
     * other despite "sharing" the lock. It only pays when the critical section is long enough for
     * the parallelism to outweigh that, and a map lookup is not.
     */
    static final class ReadWriteLockStore implements PositionStore {
        private final Map<String, Long> map = new HashMap<>();
        private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

        @Override
        public boolean accept(String vehicleId, long timestampMillis) {
            lock.readLock().lock();
            Long existing;
            try {
                existing = map.get(vehicleId);
            } finally {
                lock.readLock().unlock();
            }
            if (existing != null && existing >= timestampMillis) {
                return false;
            }
            // Not atomic with the check above - the lock was released in between. Re-checking
            // under the write lock is what makes it correct, and is easy to forget.
            lock.writeLock().lock();
            try {
                Long current = map.get(vehicleId);
                if (current != null && current >= timestampMillis) {
                    return false;
                }
                map.put(vehicleId, timestampMillis);
                return true;
            } finally {
                lock.writeLock().unlock();
            }
        }

        @Override
        public Long get(String vehicleId) {
            lock.readLock().lock();
            try {
                return map.get(vehicleId);
            } finally {
                lock.readLock().unlock();
            }
        }
    }

    /**
     * {@link StampedLock}, used <b>pessimistically</b> - and that restraint is the point.
     *
     * <p>StampedLock's headline feature is the optimistic read: grab a stamp, read, then validate
     * that nobody wrote meanwhile. It is superb for reading a handful of fields into locals. It is
     * <em>unsafe over a HashMap</em>, because an optimistic reader can observe the table mid-resize
     * and follow a half-written reference - historically an infinite loop, and in any case a read
     * of a structure that is not in a legal state. Validation happens after the damage.
     *
     * <p>So the rule is: optimistic reads are for a few fields you copy out and check, never for
     * traversing a mutable data structure. {@link Health} is where that technique belongs; here the
     * lock is used as a plain, slightly cheaper read/write lock.
     */
    static final class StampedLockStore implements PositionStore {
        private final Map<String, Long> map = new HashMap<>();
        private final StampedLock lock = new StampedLock();

        @Override
        public boolean accept(String vehicleId, long timestampMillis) {
            long stamp = lock.writeLock();
            try {
                Long existing = map.get(vehicleId);
                if (existing != null && existing >= timestampMillis) {
                    return false;
                }
                map.put(vehicleId, timestampMillis);
                return true;
            } finally {
                lock.unlockWrite(stamp);
            }
        }

        @Override
        public Long get(String vehicleId) {
            long stamp = lock.readLock();
            try {
                return map.get(vehicleId);
            } finally {
                lock.unlockRead(stamp);
            }
        }
    }
}
