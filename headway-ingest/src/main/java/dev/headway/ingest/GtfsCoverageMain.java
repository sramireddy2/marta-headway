package dev.headway.ingest;

import dev.headway.common.VehiclePosition;
import dev.headway.gtfs.GtfsSnapshot;
import dev.headway.gtfs.GtfsStaticRepository;
import dev.headway.gtfs.TripContext;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Answers the question step 6 depends on: <b>how many live vehicles can we actually place on a
 * route shape?</b>
 *
 * <p>Every vehicle whose trip cannot be resolved is a vehicle that has no direction and no path,
 * and therefore cannot take part in a headway calculation at all. If that fraction were large, the
 * whole approach would be in trouble — and it is far better to find that out now than after
 * building a Spark job on top of it.
 *
 * <pre>{@code .\mvnw.cmd -q -pl headway-ingest -am package exec:java -DskipTests "-Dexec.mainClass=dev.headway.ingest.GtfsCoverageMain"}</pre>
 */
public final class GtfsCoverageMain {

    private static final Logger log = LoggerFactory.getLogger(GtfsCoverageMain.class);

    public static void main(String[] args) throws Exception {
        try (GtfsStaticRepository repository = GtfsStaticRepository.marta(Path.of("data", "gtfs"))) {
            GtfsSnapshot gtfs = repository.snapshot();
            log.info("Static feed: {}", gtfs);

            List<VehiclePosition> live = new GtfsRealtimeClient(
                    GtfsRealtimeClient.MARTA_VEHICLE_POSITIONS).fetchVehiclePositions();
            log.info("Live feed: {} vehicles", live.size());

            int resolved = 0;
            int unknownTrip = 0;
            int unknownRoute = 0;
            int noTripId = 0;
            Map<String, Integer> unresolvedByRoute = new TreeMap<>();

            for (VehiclePosition vp : live) {
                if (vp.tripId() == null || vp.tripId().isBlank()) {
                    noTripId++;
                    unresolvedByRoute.merge(vp.routeId(), 1, Integer::sum);
                    continue;
                }
                Optional<TripContext> context = gtfs.resolve(vp.routeId(), vp.tripId());
                if (context.isPresent()) {
                    resolved++;
                    continue;
                }
                if (gtfs.trip(vp.tripId()).isEmpty()) {
                    unknownTrip++;
                } else if (gtfs.routeForRealtimeId(vp.routeId()).isEmpty()) {
                    unknownRoute++;
                }
                unresolvedByRoute.merge(vp.routeId(), 1, Integer::sum);
            }

            double pct = live.isEmpty() ? 0 : 100.0 * resolved / live.size();
            log.info("");
            log.info("RESOLVED {} of {} vehicles ({}%)", resolved, live.size(), "%.1f".formatted(pct));
            log.info("  no trip_id in the realtime feed : {}", noTripId);
            log.info("  trip_id not in trips.txt        : {}", unknownTrip);
            log.info("  route short name not in routes  : {}", unknownRoute);

            if (!unresolvedByRoute.isEmpty()) {
                log.info("  unresolved by route: {}", unresolvedByRoute);
            }

            // Direction split, which is the thing step 3 could not determine at all.
            Map<String, Long> byDirection = live.stream()
                    .map(vp -> gtfs.resolve(vp.routeId(), vp.tripId()))
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .collect(Collectors.groupingBy(
                            c -> "direction " + c.trip().directionId(), Collectors.counting()));
            log.info("  direction split: {}", byDirection);

            log.info("");
            log.info("Sample of 5 resolved vehicles:");
            live.stream()
                    .map(vp -> gtfs.resolve(vp.routeId(), vp.tripId())
                            .map(c -> "   %s -> route %s (%s) | dir %d | %s | shape %s, %.1f km"
                                    .formatted(vp.vehicleId(), c.route().shortName(),
                                            c.route().longName(), c.trip().directionId(),
                                            c.trip().headsign(), c.shape().shapeId(),
                                            c.shape().lengthMetres() / 1000)))
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .limit(5)
                    .forEach(log::info);
        }
    }

    private GtfsCoverageMain() {}
}
