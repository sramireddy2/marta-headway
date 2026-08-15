package dev.headway.api.state;

import dev.headway.common.VehiclePosition;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import org.springframework.stereotype.Component;

/**
 * Where every bus was last seen — the layer the map in step 10 draws.
 *
 * <p>Deliberately a near-copy of {@code VehicleStore} from step 2 rather than a shared class. The
 * two look alike but answer to different owners: the ingest store exists to decide what to publish
 * and evicts on a policy tied to the poll interval, while this one exists to answer HTTP requests
 * and evicts on a policy tied to what a map should still be drawing. Coupling them would mean a
 * change to the dashboard's idea of "stale" silently altering what gets written to Kafka.
 *
 * <p>What is genuinely shared is the rule: apply an update only if it is strictly newer for that
 * vehicle, inside one atomic {@code compute}. That makes replay harmless — restart the API with
 * {@code earliest} offsets and it converges to exactly the same state as a live tail.
 */
@Component
public final class VehicleState {

    private final ConcurrentHashMap<String, VehiclePosition> byVehicle = new ConcurrentHashMap<>();
    private final LongAdder applied = new LongAdder();
    private final LongAdder ignoredAsOld = new LongAdder();

    public boolean accept(VehiclePosition incoming) {
        if (incoming == null) {
            return false;
        }
        boolean[] took = {false};
        byVehicle.compute(incoming.vehicleId(), (id, existing) -> {
            if (existing != null && !existing.isSupersededBy(incoming)) {
                return existing;
            }
            took[0] = true;
            return incoming;
        });

        if (took[0]) {
            applied.increment();
        } else {
            ignoredAsOld.increment();
        }
        return took[0];
    }

    public Collection<VehiclePosition> all() {
        return List.copyOf(byVehicle.values());
    }

    /** Just the buses on one route, for the per-route view. */
    public List<VehiclePosition> onRoute(String routeId) {
        return byVehicle.values().stream()
                .filter(position -> position.routeId().equals(routeId))
                .toList();
    }

    public int evictStale(Instant now, Duration maxAge) {
        Instant cutoff = now.minus(maxAge);
        int before = byVehicle.size();
        byVehicle.values().removeIf(position -> position.timestamp().isBefore(cutoff));
        return before - byVehicle.size();
    }

    public int size() {
        return byVehicle.size();
    }

    public long appliedCount() {
        return applied.sum();
    }

    public long ignoredCount() {
        return ignoredAsOld.sum();
    }
}
