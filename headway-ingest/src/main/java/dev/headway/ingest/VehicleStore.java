package dev.headway.ingest;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import dev.headway.common.VehiclePosition;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The live answer to "where is every bus right now?", safe for many threads at once.
 *
 * <h2>The concurrency problem this solves</h2>
 *
 * A few threads write (the feed pollers) and many threads read (later: the REST API, the WebSocket
 * broadcaster, the metrics reporter). The obvious implementation is wrong in two separate ways, and
 * both are worth understanding because they are the classic mistakes.
 *
 * <p><b>Wrong attempt 1 — {@code HashMap}.</b> A plain {@link java.util.HashMap} under concurrent
 * writes does not merely lose an update; it can corrupt its internal bucket array and leave a
 * reader spinning forever inside {@code get()}. It fails silently, in production, under load.
 *
 * <p><b>Wrong attempt 2 — {@code ConcurrentHashMap} used carelessly.</b> This looks safe and is not:
 *
 * <pre>{@code
 * VehiclePosition existing = map.get(id);                       // (1)
 * if (existing == null || existing.isSupersededBy(incoming)) {
 *     map.put(id, incoming);                                    // (2)
 * }
 * }</pre>
 *
 * Each individual call is atomic, but the <em>sequence</em> is not. Two threads can both execute
 * (1), both decide to write, and then race on (2) — and nothing guarantees the newer ping lands
 * last. You end up serving a stale position with no error anywhere. This is a check-then-act race,
 * and "the collection is thread-safe" does not protect you from it.
 *
 * <p><b>The fix</b> is {@link ConcurrentHashMap#compute}: read, decide, and write as one indivisible
 * operation. While the lambda runs, that key's bin is locked, so no other thread can interleave.
 * Other keys stay fully concurrent, which is the whole appeal — one bus's update never blocks
 * another bus's update.
 *
 * <h2>Idempotency</h2>
 *
 * The feed re-sends the same ping, and pings arrive out of order. Every write is gated on
 * {@link VehiclePosition#isSupersededBy}, so applying the same message a hundred times leaves
 * exactly the state one delivery would. That property is what lets us replay a Kafka topic in
 * step 3 without corrupting anything.
 */
public final class VehicleStore {

    private static final Logger log = LoggerFactory.getLogger(VehicleStore.class);

    /**
     * How far into the future a timestamp may be before we refuse it.
     *
     * <p>Vehicle timestamps come from the bus, not the server, so a transponder with a drifting
     * clock can report the future. A future-dated ping is uniquely poisonous: no real reading ever
     * looks newer than it, so it can never be updated, and it never falls behind the eviction
     * cutoff, so it can never be swept. It would sit in the store corrupting that route's headway
     * until the process restarts.
     *
     * <p>A live sample of 185 MARTA vehicles contained none, so this is a guard against a failure
     * that is rare but permanent and silent — the combination worth spending two lines on.
     */
    private static final Duration MAX_CLOCK_SKEW = Duration.ofMinutes(2);

    /** What happened to one incoming ping. */
    public enum Outcome {
        /** First time we have ever seen this vehicle. */
        NEW,
        /** A strictly newer reading replaced the one we had. */
        UPDATED,
        /** A duplicate or out-of-order reading. Dropped. This is normal, not an error. */
        STALE,
        /** Older than {@code maxAge}, or implausibly future-dated. Never enters the store. */
        REJECTED
    }

    private final ConcurrentHashMap<String, VehiclePosition> byVehicleId = new ConcurrentHashMap<>();
    private final Clock clock;
    private final Duration maxAge;

    // LongAdder, not AtomicLong. Under contention AtomicLong has every thread compare-and-swapping
    // the same memory address, and they livelock each other. LongAdder keeps per-thread cells and
    // only sums them when you read. Write-heavy counters should always be LongAdder.
    private final LongAdder appliedCount = new LongAdder();
    private final LongAdder staleCount = new LongAdder();
    private final LongAdder rejectedCount = new LongAdder();
    private final LongAdder evictedCount = new LongAdder();

    /**
     * @param maxAge how stale a reading may be and still be usable. This one value governs
     *     <em>both</em> admission and eviction — see {@link #isAdmissible}.
     */
    public VehicleStore(Duration maxAge) {
        this(Clock.systemUTC(), maxAge);
    }

    /** The {@link Clock} is injected so tests can control "now" instead of sleeping. */
    public VehicleStore(Clock clock, Duration maxAge) {
        this.clock = clock;
        this.maxAge = maxAge;
    }

    /**
     * Is this reading fresh enough to be worth storing?
     *
     * <p><b>Why this exists.</b> Eviction and admission must agree, and originally they did not.
     * Eviction dropped anything older than {@code maxAge}, but {@code apply} would happily admit
     * any vehicle id it had not seen — including one whose GPS froze an hour ago. A bus with a
     * stuck transponder stays listed in the feed forever with an unchanging timestamp, so the
     * observed behaviour was a loop: the sweep evicted it, the next poll re-added it as
     * {@code NEW}, the next sweep evicted it again, once a minute, indefinitely.
     *
     * <p>Both rules now read the same {@code maxAge} off the same object with the same
     * {@link Clock}, so the invariant holds by construction: <b>nothing can be admitted that the
     * next sweep would immediately remove.</b> They cannot drift apart, because there is only one
     * of them.
     */
    private boolean isAdmissible(VehiclePosition p) {
        Instant now = clock.instant();
        return !p.timestamp().isBefore(now.minus(maxAge))
                && !p.timestamp().isAfter(now.plus(MAX_CLOCK_SKEW));
    }

    /**
     * Applies one ping, keeping whichever reading is newer.
     *
     * <p>Safe to call from any number of threads simultaneously.
     */
    public Outcome apply(VehiclePosition incoming) {
        if (!isAdmissible(incoming)) {
            rejectedCount.increment();
            return Outcome.REJECTED;
        }

        // Fast path: a lock-free read that can only ever prove the answer is STALE.
        //
        // compute() takes the key's bin lock even when it decides to do nothing, and doing nothing
        // is the single most common outcome here. Measured live: 3,090 pings processed, 1,565 of
        // them stale - about half, because the feed republishes every bus on every poll whether or
        // not it has moved. Skipping the lock on that path is worth 7.5x at 8 threads, 430 ops/us
        // against 57 (see headway-bench).
        //
        // WHY THIS IS NOT THE CHECK-THEN-ACT RACE THIS CLASS EXISTS TO AVOID.
        //
        // The difference is that a racy decision is *final* and this one is only a hint that can
        // skip work. The guard is deliberately written as "prove it is NOT newer", never "it is
        // newer": if the read says the stored reading is at least as new, that conclusion is
        // permanent, because the only writer is the compute() below and it never replaces a value
        // with an older one. The stored timestamp for a key is monotonically non-decreasing, so a
        // concurrent write can only make `incoming` more stale, never less.
        //
        // Every other outcome falls through to compute(), which re-reads under the bin lock and
        // makes the real decision. A stale read here therefore costs one unnecessary compute() -
        // never a lost update.
        VehiclePosition seen = byVehicleId.get(incoming.vehicleId());
        if (seen != null && !seen.isSupersededBy(incoming)) {
            staleCount.increment();
            return Outcome.STALE;
        }

        // We need to know which branch the lambda took. AtomicReference is the carrier.
        // This is safe specifically because compute() runs the function exactly once, under the
        // bin lock — it never retries it the way a compare-and-swap loop would.
        AtomicReference<Outcome> outcome = new AtomicReference<>();

        byVehicleId.compute(incoming.vehicleId(), (id, existing) -> {
            if (existing == null) {
                outcome.set(Outcome.NEW);
                return incoming;
            }
            if (existing.isSupersededBy(incoming)) {
                outcome.set(Outcome.UPDATED);
                return incoming;
            }
            outcome.set(Outcome.STALE);
            return existing; // Returning the old value leaves the map untouched.
        });

        Outcome result = outcome.get();
        if (result == Outcome.STALE) {
            staleCount.increment();
        } else {
            appliedCount.increment();
        }
        return result;
    }

    /** Applies a whole feed's worth of pings and reports the tally. */
    public Stats applyAll(Iterable<VehiclePosition> positions) {
        long fresh = 0, updated = 0, stale = 0, rejected = 0;
        for (VehiclePosition p : positions) {
            switch (apply(p)) {
                case NEW -> fresh++;
                case UPDATED -> updated++;
                case STALE -> stale++;
                case REJECTED -> rejected++;
            }
        }
        return new Stats(fresh, updated, stale, rejected);
    }

    /** Tally for a single batch. */
    public record Stats(long created, long updated, long stale, long rejected) {
        public long total() {
            return created + updated + stale + rejected;
        }
    }

    /**
     * An immutable, point-in-time copy of every vehicle.
     *
     * <p>Why copy instead of exposing the live map? Two reasons. A caller holding the real map
     * could mutate our state. And {@code ConcurrentHashMap}'s iterators are <em>weakly
     * consistent</em>: they never throw, but they may or may not reflect writes that happen while
     * you iterate. For a headway calculation that is unacceptable — you would be comparing bus A at
     * 10:00:01 against bus B at 10:00:04 and calling the difference a gap. A snapshot freezes one
     * coherent view.
     *
     * <p>The cost is O(n) per call, which for a few thousand buses is microseconds.
     */
    public ImmutableMap<String, VehiclePosition> snapshot() {
        return ImmutableMap.copyOf(byVehicleId);
    }

    /** Every vehicle currently on one route, ordered by id so the output is deterministic. */
    public ImmutableList<VehiclePosition> onRoute(String routeId) {
        return byVehicleId.values().stream()
                .filter(vp -> vp.routeId().equals(routeId))
                .sorted(Comparator.comparing(VehiclePosition::vehicleId))
                .collect(ImmutableList.toImmutableList());
    }

    /**
     * Forgets vehicles that have not reported within {@code maxAge}.
     *
     * <p>Without this the map only ever grows. A bus that finishes its shift stops appearing in the
     * feed but never sends a "goodbye", so its last position would sit here forever, and step 7
     * would happily compute a headway against a bus that went home three hours ago.
     *
     * <p>Takes no threshold argument on purpose: it uses the same {@code maxAge} that
     * {@link #isAdmissible} enforces. Passing a different one at the call site is exactly how the
     * two rules drifted apart in the first place.
     *
     * @return how many were removed
     */
    public int evictStale() {
        Instant cutoff = clock.instant().minus(maxAge);
        int before = byVehicleId.size();

        // values().removeIf() on a ConcurrentHashMap is safe to run while others read and write.
        byVehicleId.values().removeIf(vp -> vp.timestamp().isBefore(cutoff));

        int removed = before - byVehicleId.size();
        if (removed > 0) {
            evictedCount.add(removed);
            log.debug("Evicted {} vehicles not seen since {}", removed, cutoff);
        }
        return removed;
    }

    public int size() {
        return byVehicleId.size();
    }

    public long appliedTotal() {
        return appliedCount.sum();
    }

    public long staleTotal() {
        return staleCount.sum();
    }

    public long rejectedTotal() {
        return rejectedCount.sum();
    }

    public long evictedTotal() {
        return evictedCount.sum();
    }

    /** Distinct routes with at least one live vehicle. */
    public int routeCount() {
        return (int) byVehicleId.values().stream().map(VehiclePosition::routeId).distinct().count();
    }

    /** Package-private hook for tests that need to start from empty. */
    void clear() {
        byVehicleId.clear();
    }

    @Override
    public String toString() {
        return "VehicleStore[vehicles=%d, routes=%d, applied=%d, stale=%d, rejected=%d, evicted=%d]"
                .formatted(size(), routeCount(), appliedTotal(), staleTotal(),
                        rejectedTotal(), evictedTotal());
    }
}
