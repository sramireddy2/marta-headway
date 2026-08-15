package dev.headway.api.state;

import dev.headway.api.model.RouteHeadway;
import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import org.springframework.stereotype.Component;

/**
 * The current headway for every route-direction group: one row per group, always the newest.
 *
 * <h2>Why this is not just a map assignment</h2>
 *
 * Records arrive from a topic fed by sliding windows, so for one group the API sees the window
 * ending 10:01:00, then 10:01:30, then 10:02:00 — and, because Spark's Update output mode re-emits
 * a window whenever late data refines it, sometimes 10:01:30 again <em>after</em> 10:02:00. A naive
 * {@code map.put} would let that stale re-emission overwrite a newer measurement, and the dashboard
 * would jump backwards in time for no visible reason.
 *
 * <p>So the write is gated on event time, and the gate has to be inside the same atomic operation
 * as the write. {@code if (newer) map.put(...)} is a check-then-act race: two consumer threads can
 * both read the old value, both decide they are newer, and the older of the two can land second.
 * There is only one consumer thread today, but the correctness of this class should not depend on
 * that — {@code compute()} makes read-decide-write one indivisible step, at no cost.
 *
 * <p>This is the same reasoning as {@code VehicleStore} in step 2, applied to a different clock:
 * there the gate was the GPS timestamp, here it is the window's end.
 */
@Component
public final class LiveHeadwayState {

    private final ConcurrentHashMap<String, RouteHeadway> byGroup = new ConcurrentHashMap<>();
    private final LongAdder accepted = new LongAdder();
    private final LongAdder rejectedAsStale = new LongAdder();

    /**
     * Records a measurement, keeping it only if it is at least as recent as what is already held.
     *
     * @return true if this record became the current state for its group
     */
    public boolean accept(RouteHeadway incoming) {
        if (incoming == null || incoming.headwayGroup() == null || incoming.windowEnd() == null) {
            return false;
        }

        // A lambda cannot assign to a local variable, so a one-element array carries the decision
        // out of the compute block. Ugly, but the alternative - deciding outside and writing inside
        // - is exactly the race this method exists to avoid.
        boolean[] took = {false};
        byGroup.compute(incoming.headwayGroup(), (group, existing) -> {
            if (existing != null && incoming.windowEnd().isBefore(existing.windowEnd())) {
                return existing;
            }
            took[0] = true;
            return incoming;
        });

        if (took[0]) {
            accepted.increment();
        } else {
            rejectedAsStale.increment();
        }
        return took[0];
    }

    public Optional<RouteHeadway> group(String headwayGroup) {
        return Optional.ofNullable(byGroup.get(headwayGroup));
    }

    /**
     * Every group currently known, worst first.
     *
     * <p>Ordered by how far the ratio is from 1.0, so the routes needing attention are at the top
     * without the caller having to know the classification rules. Groups with no schedule to
     * compare against sort last rather than being dropped — "we cannot say" is different from
     * "fine", and hiding them would make the route list disagree with the vehicle count.
     */
    public List<RouteHeadway> current() {
        return byGroup.values().stream()
                .sorted(Comparator
                        .comparingDouble(LiveHeadwayState::disorder).reversed()
                        .thenComparing(RouteHeadway::headwayGroup))
                .toList();
    }

    private static double disorder(RouteHeadway headway) {
        Double ratio = headway.headwayRatio();
        if (ratio == null || !headway.isAlertable()) {
            return -1;
        }
        return Math.abs(ratio - 1.0);
    }

    /**
     * Forgets groups whose newest window is older than {@code maxAge}.
     *
     * <p>Without this the map is a leak with a friendly name. A route that stops running for the
     * night would sit in the dashboard until the process restarts, showing a headway measured
     * hours ago as though it were current — worse than showing nothing.
     *
     * @return how many groups were dropped
     */
    public int evictStale(Instant now, Duration maxAge) {
        Instant cutoff = now.minus(maxAge);
        int before = byGroup.size();
        // removeIf on a ConcurrentHashMap's value view is safe under concurrent modification: it
        // iterates weakly-consistently and removes via the entry, not via a snapshot.
        byGroup.values().removeIf(headway -> headway.windowEnd().isBefore(cutoff));
        return before - byGroup.size();
    }

    public int size() {
        return byGroup.size();
    }

    public long acceptedCount() {
        return accepted.sum();
    }

    public long staleCount() {
        return rejectedAsStale.sum();
    }

    /** The most recent window end across all groups, or empty if nothing has arrived. */
    public Optional<Instant> newestWindowEnd() {
        return byGroup.values().stream().map(RouteHeadway::windowEnd).max(Instant::compareTo);
    }
}
