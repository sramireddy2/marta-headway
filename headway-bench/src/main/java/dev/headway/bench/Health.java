package dev.headway.bench;

import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.StampedLock;

/**
 * Five ways to read four related fields and be sure they came from the same update.
 *
 * <p>This is the shape of a route's live health - window end, ratio, vehicle count, status - where
 * a reader that mixes a new ratio with an old vehicle count reports something that was never true.
 * A single {@code ConcurrentHashMap} entry does not help here: the problem is not the map, it is
 * that four values have to move together.
 *
 * <h2>The invariant, and how tearing is detected</h2>
 *
 * Every field is derived from one monotonically increasing {@code seq}, so a reader can check its
 * own consistency: if {@code vehicleCount} does not match {@code windowEnd}, the read was torn.
 * That turns "is this safe?" from an argument into a counter, which is the only version worth
 * having.
 */
final class Health {

    private Health() {}

    interface RouteHealth {

        void update(long seq);

        /** @return the reconstructed seq, or -1 if the four fields disagreed */
        long read();
    }

    /* The four fields, as functions of one sequence number. */
    private static int countOf(long seq) {
        return (int) (seq & 7);
    }

    private static double ratioOf(long seq) {
        return seq * 3.0;
    }

    private static int statusOf(long seq) {
        return (int) (seq % 5);
    }

    private static long check(long windowEnd, int count, double ratio, int status) {
        return count == countOf(windowEnd) && ratio == ratioOf(windowEnd) && status == statusOf(windowEnd)
                ? windowEnd
                : -1;
    }

    /**
     * No synchronisation at all. Included to prove the problem is real.
     *
     * <p>Expect torn reads. Also expect suspiciously high throughput, because with non-volatile
     * fields the JIT is entitled to hoist the loads out of the loop and never observe another
     * thread's writes again - a reader can be fast, wrong, and permanently stale at once.
     */
    static final class Plain implements RouteHealth {
        private long windowEnd;
        private int vehicleCount;
        private double ratio;
        private int status;

        @Override
        public void update(long seq) {
            windowEnd = seq;
            vehicleCount = countOf(seq);
            ratio = ratioOf(seq);
            status = statusOf(seq);
        }

        @Override
        public long read() {
            return check(windowEnd, vehicleCount, ratio, status);
        }
    }

    static final class Synchronized implements RouteHealth {
        private long windowEnd;
        private int vehicleCount;
        private double ratio;
        private int status;

        @Override
        public synchronized void update(long seq) {
            windowEnd = seq;
            vehicleCount = countOf(seq);
            ratio = ratioOf(seq);
            status = statusOf(seq);
        }

        @Override
        public synchronized long read() {
            return check(windowEnd, vehicleCount, ratio, status);
        }
    }

    /** Readers do not block each other, but they do all write to the same lock word. */
    static final class RwLock implements RouteHealth {
        private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        private long windowEnd;
        private int vehicleCount;
        private double ratio;
        private int status;

        @Override
        public void update(long seq) {
            lock.writeLock().lock();
            try {
                windowEnd = seq;
                vehicleCount = countOf(seq);
                ratio = ratioOf(seq);
                status = statusOf(seq);
            } finally {
                lock.writeLock().unlock();
            }
        }

        @Override
        public long read() {
            lock.readLock().lock();
            try {
                return check(windowEnd, vehicleCount, ratio, status);
            } finally {
                lock.readLock().unlock();
            }
        }
    }

    /**
     * {@link StampedLock} used the way it was designed to be used.
     *
     * <p>A reader takes a stamp, copies the four fields into locals, and then asks whether a write
     * happened meanwhile. In the common case - no writer - it never writes to the lock at all,
     * which is exactly what a read/write lock cannot avoid. When validation fails it falls back to
     * a real read lock rather than spinning, so a busy writer cannot starve it.
     *
     * <p>Two rules make this safe and both are easy to get wrong. The fields must be copied to
     * locals <em>before</em> validating, never used directly afterwards. And the values may be
     * garbage until {@code validate} returns true, so nothing between the stamp and the check may
     * dereference them, index an array with them, or loop on them.
     */
    static final class StampedOptimistic implements RouteHealth {
        private final StampedLock lock = new StampedLock();
        private long windowEnd;
        private int vehicleCount;
        private double ratio;
        private int status;

        @Override
        public void update(long seq) {
            long stamp = lock.writeLock();
            try {
                windowEnd = seq;
                vehicleCount = countOf(seq);
                ratio = ratioOf(seq);
                status = statusOf(seq);
            } finally {
                lock.unlockWrite(stamp);
            }
        }

        @Override
        public long read() {
            long stamp = lock.tryOptimisticRead();
            long w = windowEnd;
            int c = vehicleCount;
            double r = ratio;
            int s = status;
            if (!lock.validate(stamp)) {
                stamp = lock.readLock();
                try {
                    w = windowEnd;
                    c = vehicleCount;
                    r = ratio;
                    s = status;
                } finally {
                    lock.unlockRead(stamp);
                }
            }
            return check(w, c, r, s);
        }
    }

    /**
     * No lock: publish an immutable record and swap the reference.
     *
     * <p>A reader performs one volatile read and then holds an object nobody can change. There is
     * no window to tear in, because the four fields were never separately visible. This is the
     * pattern the rest of the project already uses - {@code VehiclePosition}, {@code GtfsSnapshot},
     * {@code RouteHeadway} are all immutable records shared without locking - so the benchmark is
     * really asking whether that habit was worth having.
     *
     * <p>The cost is one allocation per <em>write</em>, which is the right trade when reads vastly
     * outnumber writes. Here that ratio is 7:1; on the real dashboard it is far higher.
     */
    static final class ImmutableSnapshot implements RouteHealth {
        private record Snapshot(long windowEnd, int vehicleCount, double ratio, int status) {}

        private final AtomicReference<Snapshot> ref =
                new AtomicReference<>(new Snapshot(0, countOf(0), ratioOf(0), statusOf(0)));

        @Override
        public void update(long seq) {
            ref.set(new Snapshot(seq, countOf(seq), ratioOf(seq), statusOf(seq)));
        }

        @Override
        public long read() {
            Snapshot snapshot = ref.get();
            return check(snapshot.windowEnd(), snapshot.vehicleCount(), snapshot.ratio(),
                    snapshot.status());
        }
    }
}
