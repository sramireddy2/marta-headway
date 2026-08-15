package dev.headway.ingest;

import dev.headway.common.VehiclePosition;
import dev.headway.gtfs.GtfsSnapshot;
import dev.headway.gtfs.GtfsStaticRepository;
import dev.headway.gtfs.ShapeProjection;
import dev.headway.gtfs.ShapeProjector;
import dev.headway.gtfs.TripContext;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Checks the projection against an independent measurement, rather than against itself.
 *
 * <p>The self-consistency test in {@link HeadwayPreviewMain} proves the geometry is internally
 * correct: a vertex projects back to its own distance. What it cannot prove is that the numbers
 * mean anything about the real world. A projector with the wrong distance units, or one picking
 * plausible-but-wrong segments, would still pass it.
 *
 * <p>So this compares two quantities that share no code at all:
 *
 * <ul>
 *   <li><b>Implied speed</b> — the change in our computed distance-along-route between two
 *       sightings, divided by the elapsed time. Derived entirely from our geometry and the shape
 *       file.
 *   <li><b>Reported speed</b> — what the bus's own equipment put in the feed. Derived from
 *       hardware on the vehicle, and never touched by any of our code.
 *   </ul>
 *
 * If those agree, the projection is measuring real movement along a real road. If the projector
 * were off by a factor of 1000 (kilometres read as metres), picking wrong segments, or
 * interpolating incorrectly, the two would diverge immediately and obviously.
 *
 * <p>Also checks physical plausibility: a city bus does not travel at 200 m/s, and it very rarely
 * moves backwards along its own route.
 *
 * <pre>{@code .\mvnw.cmd -q -pl headway-ingest -am package exec:java -DskipTests "-Dexec.mainClass=dev.headway.ingest.ProjectionAccuracyMain"}</pre>
 */
public final class ProjectionAccuracyMain {

    private static final Logger log = LoggerFactory.getLogger(ProjectionAccuracyMain.class);

    /** Long enough that a moving bus covers real ground, short enough to stay a quick check. */
    private static final Duration GAP_BETWEEN_SAMPLES = Duration.ofSeconds(75);

    /** How far a bus could plausibly move between sightings, for the hinted projection. */
    private static final double HINT_WINDOW_METRES = 3_000;

    /** Beyond this the fix is not on the route we think it is; excluded from the comparison. */
    private static final double MAX_CROSS_TRACK_METRES = 150;

    private record Sighting(VehiclePosition vehicle, TripContext context, ShapeProjection projection) {}

    public static void main(String[] args) throws Exception {
        try (GtfsStaticRepository repository = GtfsStaticRepository.marta(Path.of("data", "gtfs"))) {
            GtfsSnapshot gtfs = repository.snapshot();
            GtfsRealtimeClient client =
                    new GtfsRealtimeClient(GtfsRealtimeClient.MARTA_VEHICLE_POSITIONS);

            log.info("Sample 1 ...");
            Map<String, Sighting> first = sample(gtfs, client, null);
            log.info("  {} vehicles projected on route", first.size());

            log.info("Waiting {}s for the buses to move ...", GAP_BETWEEN_SAMPLES.toSeconds());
            Thread.sleep(GAP_BETWEEN_SAMPLES.toMillis());

            log.info("Sample 2 ...");
            Map<String, Sighting> second = sample(gtfs, client, first);
            log.info("  {} vehicles projected on route", second.size());

            compare(first, second);
        }
    }

    /** Projects the current feed, using the previous sample as a hint where one exists. */
    private static Map<String, Sighting> sample(GtfsSnapshot gtfs, GtfsRealtimeClient client,
                                                Map<String, Sighting> previous) throws Exception {
        Map<String, Sighting> out = new HashMap<>();
        for (VehiclePosition vp : client.fetchVehiclePositions()) {
            Optional<TripContext> context = gtfs.resolve(vp.routeId(), vp.tripId());
            if (context.isEmpty()) {
                continue;
            }
            TripContext c = context.get();

            Sighting before = previous == null ? null : previous.get(vp.vehicleId());
            ShapeProjection projection;
            if (before != null && before.context().shape().shapeId().equals(c.shape().shapeId())) {
                // The call the real pipeline makes: anchored to where this bus was last seen.
                projection = ShapeProjector.projectNear(c.shape(), vp.latitude(), vp.longitude(),
                        before.projection().distanceAlongRouteMetres(), HINT_WINDOW_METRES);
            } else {
                projection = ShapeProjector.project(c.shape(), vp.latitude(), vp.longitude());
            }

            if (projection.isOnRoute(MAX_CROSS_TRACK_METRES)) {
                out.put(vp.vehicleId(), new Sighting(vp, c, projection));
            }
        }
        return out;
    }

    private static void compare(Map<String, Sighting> first, Map<String, Sighting> second) {
        List<Double> impliedSpeeds = new ArrayList<>();
        List<double[]> speedPairs = new ArrayList<>(); // {implied, reported}
        int moved = 0;
        int stationary = 0;
        int backwards = 0;
        int implausible = 0;
        int tripChanged = 0;

        for (Map.Entry<String, Sighting> entry : second.entrySet()) {
            Sighting now = entry.getValue();
            Sighting before = first.get(entry.getKey());
            if (before == null) {
                continue;
            }
            if (!before.context().shape().shapeId().equals(now.context().shape().shapeId())) {
                tripChanged++; // started a new run; distances are not comparable
                continue;
            }

            double seconds = Duration.between(
                    before.vehicle().timestamp(), now.vehicle().timestamp()).toMillis() / 1000.0;
            if (seconds < 5) {
                stationary++; // the GPS did not refresh between our two fetches
                continue;
            }

            double deltaMetres = now.projection().distanceAlongRouteMetres()
                    - before.projection().distanceAlongRouteMetres();
            double impliedSpeed = deltaMetres / seconds;
            moved++;

            if (deltaMetres < -20) {
                backwards++;
            }
            if (Math.abs(impliedSpeed) > 35) { // 126 km/h; no MARTA bus does this
                implausible++;
            }
            impliedSpeeds.add(impliedSpeed);

            // The independent comparison. Average the two reported instantaneous speeds as the
            // best available estimate of the average speed over the interval.
            if (before.vehicle().speed().isPresent() && now.vehicle().speed().isPresent()) {
                double reported = (before.vehicle().speed().getAsDouble()
                        + now.vehicle().speed().getAsDouble()) / 2.0;
                speedPairs.add(new double[] {impliedSpeed, reported});
            }
        }

        log.info("");
        log.info("=== movement ===");
        log.info("  vehicles seen in both samples with a fresh fix : {}", moved);
        log.info("  GPS did not refresh between samples            : {}", stationary);
        log.info("  started a different trip                       : {}", tripChanged);
        log.info("  appeared to move backwards by more than 20 m   : {} ({}%)",
                backwards, pct(backwards, moved));
        log.info("  implied speed over 35 m/s (physically absurd)  : {} ({}%)",
                implausible, pct(implausible, moved));

        if (!impliedSpeeds.isEmpty()) {
            double[] sorted = impliedSpeeds.stream().mapToDouble(Double::doubleValue).sorted().toArray();
            log.info("  implied speed: median {} m/s | p90 {} m/s | max {} m/s",
                    f(percentile(sorted, 50)), f(percentile(sorted, 90)), f(sorted[sorted.length - 1]));
        }

        log.info("");
        log.info("=== accuracy: our geometry vs the bus's own speedometer ===");
        if (speedPairs.size() < 5) {
            log.warn("  only {} vehicles reported speed in both samples; too few to judge",
                    speedPairs.size());
            return;
        }

        double[] errors = new double[speedPairs.size()];
        for (int i = 0; i < speedPairs.size(); i++) {
            errors[i] = Math.abs(speedPairs.get(i)[0] - speedPairs.get(i)[1]);
        }
        java.util.Arrays.sort(errors);

        double meanImplied = speedPairs.stream().mapToDouble(p -> p[0]).average().orElse(0);
        double meanReported = speedPairs.stream().mapToDouble(p -> p[1]).average().orElse(0);

        log.info("  vehicles reporting speed in both samples : {}", speedPairs.size());
        log.info("  mean implied speed  (our projection)     : {} m/s", f(meanImplied));
        log.info("  mean reported speed (vehicle hardware)   : {} m/s", f(meanReported));
        log.info("  correlation                              : {}", f(correlation(speedPairs)));
        log.info("  absolute difference: median {} m/s | p90 {} m/s",
                f(percentile(errors, 50)), f(percentile(errors, 90)));

        log.info("");
        log.info("  sample of 8 vehicles (implied vs reported):");
        speedPairs.stream().limit(8).forEach(p ->
                log.info("    {} m/s  vs  {} m/s", f(p[0]), f(p[1])));

        log.info("");
        double r = correlation(speedPairs);
        if (r > 0.7 && Math.abs(meanImplied - meanReported) < 3) {
            log.info("  VERDICT: the two independent measurements agree. The projection is");
            log.info("           tracking real movement along the real road.");
        } else {
            log.warn("  VERDICT: the measurements disagree. Investigate before trusting headways.");
        }
    }

    private static double correlation(List<double[]> pairs) {
        int n = pairs.size();
        double sumX = 0, sumY = 0, sumXY = 0, sumXX = 0, sumYY = 0;
        for (double[] p : pairs) {
            sumX += p[0];
            sumY += p[1];
            sumXY += p[0] * p[1];
            sumXX += p[0] * p[0];
            sumYY += p[1] * p[1];
        }
        double numerator = n * sumXY - sumX * sumY;
        double denominator = Math.sqrt((n * sumXX - sumX * sumX) * (n * sumYY - sumY * sumY));
        return denominator == 0 ? 0 : numerator / denominator;
    }

    private static double percentile(double[] sorted, int p) {
        if (sorted.length == 0) {
            return 0;
        }
        int i = Math.min(sorted.length - 1, (int) Math.ceil(p / 100.0 * sorted.length) - 1);
        return sorted[Math.max(0, i)];
    }

    private static String pct(int part, int whole) {
        return whole == 0 ? "0.0" : "%.1f".formatted(100.0 * part / whole);
    }

    private static String f(double v) {
        return "%.2f".formatted(v);
    }

    private ProjectionAccuracyMain() {}
}
