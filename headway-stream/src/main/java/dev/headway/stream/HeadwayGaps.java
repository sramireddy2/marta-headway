package dev.headway.stream;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The headway calculation itself, with no Spark in it.
 *
 * <p>Deliberately separated from {@link HeadwayFunctions}. The logic below — deduplicate, order,
 * difference — is where a bug would silently produce wrong headways, and it is ordinary Java
 * operating on ordinary values. Leaving it inside a UDF would mean the only way to test it was to
 * stand up a {@code SparkSession} and push rows through a DataFrame, which is slow enough that in
 * practice it does not get tested at all.
 *
 * <p>{@link HeadwayFunctions} keeps only the part that genuinely needs Spark: converting Catalyst
 * rows to and from these records.
 */
final class HeadwayGaps {

    private HeadwayGaps() {}

    /**
     * One vehicle seen at one moment.
     *
     * @param vehicleId which bus
     * @param distanceMetres how far along the route it had travelled
     * @param timestampMillis when the GPS fix was taken, for choosing the newest reading
     */
    record Sighting(String vehicleId, double distanceMetres, long timestampMillis) {}

    /** The gaps between consecutive buses in one route-direction group, plus summary statistics. */
    record Result(
            int vehicleCount,
            List<Double> gapsMetres,
            Double minGapMetres,
            Double medianGapMetres,
            Double maxGapMetres,
            Double meanGapMetres,
            List<String> orderedVehicles,
            List<Double> orderedDistancesMetres) {

        static Result empty() {
            return new Result(0, List.of(), null, null, null, null, List.of(), List.of());
        }
    }

    /**
     * Turns a window's worth of sightings into the gaps between consecutive buses.
     *
     * <p>The three steps, in an order that matters:
     *
     * <ol>
     *   <li><b>Keep only the newest reading per vehicle.</b> A 60-second window at 15-second
     *       polling holds roughly four sightings of the same bus. Skipping this step measures the
     *       distance between a bus and <em>itself</em> a few seconds earlier — tens of metres —
     *       and the system reports constant, severe bunching everywhere. It is the single most
     *       damaging thing that can go wrong here, which is why there is a test named after it.
     *   <li><b>Order by distance along the route.</b> Not by vehicle id and not by arrival order:
     *       position along the route is the only ordering in which "consecutive" means anything.
     *   <li><b>Difference adjacent pairs.</b> Those differences are the headways.
     * </ol>
     *
     * <p>Ties in timestamp keep the first reading seen. Two readings of one vehicle sharing a
     * timestamp are duplicates of each other, so the choice does not matter — but it must be
     * deterministic, or the same window would produce different answers on replay.
     *
     * @return statistics plus the ordered vehicles, so an alert can name which two buses are close
     */
    static Result compute(List<Sighting> sightings) {
        if (sightings == null || sightings.isEmpty()) {
            return Result.empty();
        }

        // 1. newest reading per vehicle
        Map<String, Sighting> newest = new HashMap<>();
        for (Sighting sighting : sightings) {
            Sighting previous = newest.get(sighting.vehicleId());
            if (previous == null || sighting.timestampMillis() > previous.timestampMillis()) {
                newest.put(sighting.vehicleId(), sighting);
            }
        }

        // 2. order along the route. Vehicle id breaks ties so that two buses at an identical
        //    distance - which happens at terminals - produce a stable, replayable ordering.
        List<Sighting> ordered = new ArrayList<>(newest.values());
        ordered.sort(Comparator.comparingDouble(Sighting::distanceMetres)
                .thenComparing(Sighting::vehicleId));

        List<String> vehicleIds = new ArrayList<>(ordered.size());
        List<Double> distances = new ArrayList<>(ordered.size());
        for (Sighting sighting : ordered) {
            vehicleIds.add(sighting.vehicleId());
            distances.add(sighting.distanceMetres());
        }

        // 3. differences between neighbours
        List<Double> gaps = new ArrayList<>(Math.max(0, ordered.size() - 1));
        for (int i = 1; i < distances.size(); i++) {
            gaps.add(distances.get(i) - distances.get(i - 1));
        }

        return new Result(
                ordered.size(),
                List.copyOf(gaps),
                gaps.isEmpty() ? null : gaps.stream().min(Double::compare).orElseThrow(),
                median(gaps),
                gaps.isEmpty() ? null : gaps.stream().max(Double::compare).orElseThrow(),
                gaps.isEmpty() ? null
                        : gaps.stream().mapToDouble(Double::doubleValue).average().orElseThrow(),
                List.copyOf(vehicleIds),
                List.copyOf(distances));
    }

    /** Middle value, or the mean of the middle two for an even count. Null for no values. */
    static Double median(List<Double> values) {
        if (values.isEmpty()) {
            return null;
        }
        List<Double> sorted = new ArrayList<>(values);
        sorted.sort(Double::compare);
        int middle = sorted.size() / 2;
        return sorted.size() % 2 == 1
                ? sorted.get(middle)
                : (sorted.get(middle - 1) + sorted.get(middle)) / 2.0;
    }
}
