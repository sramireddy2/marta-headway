package dev.headway.ingest;

import dev.headway.common.VehiclePosition;
import dev.headway.gtfs.GtfsSnapshot;
import dev.headway.gtfs.GtfsStaticRepository;
import dev.headway.gtfs.RouteShape;
import dev.headway.gtfs.ShapeProjection;
import dev.headway.gtfs.ShapeProjector;
import dev.headway.gtfs.TripContext;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Validates the projection against real data and shows the gaps it produces between live buses.
 *
 * <p>Three things happen here, in increasing order of interest:
 *
 * <ol>
 *   <li><b>Self-consistency.</b> Every sampled vertex of every real shape is projected back onto
 *       its own shape. A correct projector returns that vertex's own {@code shape_dist_traveled}
 *       with zero cross-track error. Any drift is a bug, measured against 215 real polylines
 *       rather than a hand-written fixture.
 *   <li><b>Live cross-track.</b> How far real GPS fixes actually land from the path they are
 *       supposed to be on. This is the number that decides what a sane off-route threshold is.
 *   <li><b>Gaps.</b> Buses grouped by route and direction, sorted by distance along the route,
 *       differenced. These are headways — the thing the project is named after.
 * </ol>
 *
 * <pre>{@code .\mvnw.cmd -q -pl headway-ingest -am package exec:java -DskipTests "-Dexec.mainClass=dev.headway.ingest.HeadwayPreviewMain"}</pre>
 */
public final class HeadwayPreviewMain {

    private static final Logger log = LoggerFactory.getLogger(HeadwayPreviewMain.class);

    /** Project every Nth vertex rather than all 359,676; the scan is quadratic in shape size. */
    private static final int VERTEX_SAMPLE_STRIDE = 40;

    public static void main(String[] args) throws Exception {
        try (GtfsStaticRepository repository = GtfsStaticRepository.marta(Path.of("data", "gtfs"))) {
            GtfsSnapshot gtfs = repository.snapshot();

            selfConsistencyCheck(gtfs);
            List<Projected> projected = projectLiveVehicles(gtfs);
            reportCrossTrack(projected);
            reportGaps(projected);
        }
    }

    private record Projected(VehiclePosition vehicle, TripContext context, ShapeProjection projection) {}

    /** A vertex projected onto its own shape must come back as itself. */
    private static void selfConsistencyCheck(GtfsSnapshot gtfs) {
        log.info("=== self-consistency: projecting real shape vertices back onto their own shape ===");

        double worstAlongError = 0;
        double worstCrossTrack = 0;
        String worstShape = "";
        long checked = 0;
        long ambiguousVertices = 0;
        long hintedFailures = 0;
        java.util.Set<String> ambiguousShapes = new java.util.TreeSet<>();
        long start = System.nanoTime();

        for (RouteShape shape : gtfs.shapesById().values()) {
            for (int i = 0; i < shape.pointCount(); i += VERTEX_SAMPLE_STRIDE) {
                double truth = shape.distanceAt(i);
                ShapeProjection p =
                        ShapeProjector.project(shape, shape.latitudeAt(i), shape.longitudeAt(i));

                double alongError = Math.abs(p.distanceAlongRouteMetres() - truth);
                if (alongError > worstAlongError) {
                    worstAlongError = alongError;
                    worstShape = shape.shapeId();
                }
                worstCrossTrack = Math.max(worstCrossTrack, p.crossTrackMetres());

                // Cross-track of zero with a large along-route error means the identical
                // coordinate appears twice on this shape: it loops or doubles back. The maths is
                // right; the question "where on the route is this?" simply has two answers.
                if (alongError > 50) {
                    ambiguousVertices++;
                    ambiguousShapes.add(shape.shapeId());

                    // The hinted variant is the fix. A real vehicle always has a previous
                    // position, so this is the call the pipeline will actually make.
                    ShapeProjection hinted = ShapeProjector.projectNear(
                            shape, shape.latitudeAt(i), shape.longitudeAt(i), truth, 400);
                    if (Math.abs(hinted.distanceAlongRouteMetres() - truth) > 50) {
                        hintedFailures++;
                    }
                }
                checked++;
            }
        }

        long millis = (System.nanoTime() - start) / 1_000_000;
        log.info("  {} vertices across {} shapes in {}ms ({} projections/sec)",
                checked, gtfs.shapeCount(), millis,
                millis == 0 ? "n/a" : String.valueOf(checked * 1000 / millis));
        log.info("  worst cross-track error   : {} m  <- the geometry itself",
                "%.4f".formatted(worstCrossTrack));
        log.info("  worst along-route error   : {} m (shape {})",
                "%.1f".formatted(worstAlongError), worstShape);
        log.info("  vertices with >50 m along-route error: {} of {} ({}%), across {} shapes",
                ambiguousVertices, checked,
                "%.2f".formatted(100.0 * ambiguousVertices / checked), ambiguousShapes.size());
        log.info("    these are self-intersections: same coordinate, two places on the route");
        log.info("    still wrong when given a previous-position hint: {}", hintedFailures);
        log.info("");
    }

    private static List<Projected> projectLiveVehicles(GtfsSnapshot gtfs) throws Exception {
        List<VehiclePosition> live = new GtfsRealtimeClient(
                GtfsRealtimeClient.MARTA_VEHICLE_POSITIONS).fetchVehiclePositions();

        List<Projected> out = new ArrayList<>(live.size());
        long start = System.nanoTime();
        for (VehiclePosition vp : live) {
            Optional<TripContext> context = gtfs.resolve(vp.routeId(), vp.tripId());
            if (context.isEmpty()) {
                continue;
            }
            TripContext c = context.get();
            out.add(new Projected(vp, c,
                    ShapeProjector.project(c.shape(), vp.latitude(), vp.longitude())));
        }
        long micros = (System.nanoTime() - start) / 1_000;

        log.info("=== live vehicles ===");
        log.info("  {} fetched, {} projected in {}us ({}us per vehicle)",
                live.size(), out.size(), micros,
                out.isEmpty() ? 0 : micros / out.size());
        return out;
    }

    private static void reportCrossTrack(List<Projected> projected) {
        if (projected.isEmpty()) {
            log.info("  no vehicles to report on");
            return;
        }
        double[] crossTrack = projected.stream()
                .mapToDouble(p -> p.projection().crossTrackMetres()).sorted().toArray();

        log.info("  cross-track distance from the route the bus is supposed to be on:");
        log.info("    median {} m | p90 {} m | p99 {} m | max {} m",
                fmt(percentile(crossTrack, 50)), fmt(percentile(crossTrack, 90)),
                fmt(percentile(crossTrack, 99)), fmt(crossTrack[crossTrack.length - 1]));

        long beyond100 = projected.stream()
                .filter(p -> p.projection().crossTrackMetres() > 100).count();
        log.info("    {} of {} vehicles are more than 100 m off route",
                beyond100, projected.size());

        projected.stream()
                .filter(p -> p.projection().crossTrackMetres() > 100)
                .sorted(Comparator.comparingDouble(
                        (Projected p) -> p.projection().crossTrackMetres()).reversed())
                .limit(3)
                .forEach(p -> log.info("      vehicle {} on route {} is {} m off shape {}",
                        p.vehicle().vehicleId(), p.context().route().shortName(),
                        fmt(p.projection().crossTrackMetres()), p.context().shape().shapeId()));
        log.info("");
    }

    /**
     * The payoff: buses grouped by route <em>and direction</em>, ordered along the route, with the
     * distance between each consecutive pair. That distance is the headway, in metres.
     */
    private static void reportGaps(List<Projected> projected) {
        Map<String, List<Projected>> byGroup = new TreeMap<>();
        for (Projected p : projected) {
            byGroup.computeIfAbsent(p.context().headwayGroup(), g -> new ArrayList<>()).add(p);
        }

        Map<String, List<Double>> gapsByGroup = new LinkedHashMap<>();
        for (Map.Entry<String, List<Projected>> entry : byGroup.entrySet()) {
            List<Projected> buses = entry.getValue();
            if (buses.size() < 2) {
                continue; // one bus has nobody to have a gap with
            }
            buses.sort(Comparator.comparingDouble(p -> p.projection().distanceAlongRouteMetres()));

            List<Double> gaps = new ArrayList<>();
            for (int i = 1; i < buses.size(); i++) {
                gaps.add(buses.get(i).projection().distanceAlongRouteMetres()
                        - buses.get(i - 1).projection().distanceAlongRouteMetres());
            }
            gapsByGroup.put(entry.getKey(), gaps);
        }

        log.info("=== gaps between consecutive buses (route:direction) ===");
        log.info("  {} groups have 2 or more buses", gapsByGroup.size());
        log.info("");

        gapsByGroup.entrySet().stream()
                .sorted(Comparator.comparingInt((Map.Entry<String, List<Double>> e)
                        -> e.getValue().size()).reversed())
                .limit(6)
                .forEach(entry -> {
                    List<Projected> buses = byGroup.get(entry.getKey());
                    log.info("  group {} — {} buses on a {} km shape",
                            entry.getKey(), buses.size(),
                            fmt(buses.getFirst().context().shape().lengthMetres() / 1000));
                    for (int i = 0; i < buses.size(); i++) {
                        Projected p = buses.get(i);
                        String gap = i == 0 ? "" :
                                "   gap %s m".formatted(fmt(entry.getValue().get(i - 1)));
                        log.info("     {} at {} m{}", p.vehicle().vehicleId(),
                                fmt(p.projection().distanceAlongRouteMetres()), gap);
                    }
                });

        log.info("");
        log.info("  tightest gaps anywhere in the system — the bunching candidates:");
        gapsByGroup.entrySet().stream()
                .flatMap(e -> e.getValue().stream().map(g -> Map.entry(e.getKey(), g)))
                .sorted(Map.Entry.comparingByValue())
                .limit(8)
                .forEach(e -> log.info("    {} m apart on group {}", fmt(e.getValue()), e.getKey()));
    }

    private static double percentile(double[] sorted, int p) {
        if (sorted.length == 0) {
            return 0;
        }
        int index = Math.min(sorted.length - 1, (int) Math.ceil(p / 100.0 * sorted.length) - 1);
        return sorted[Math.max(0, index)];
    }

    private static String fmt(double value) {
        return "%.1f".formatted(value);
    }

    private HeadwayPreviewMain() {}
}
