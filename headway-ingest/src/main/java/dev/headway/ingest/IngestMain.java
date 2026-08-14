package dev.headway.ingest;

import com.google.transit.realtime.GtfsRealtime.FeedMessage;
import dev.headway.common.VehiclePosition;
import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Step 1 entry point: fetch MARTA's live feed once, decode it, and print what is out there.
 *
 * <p>Run it with:
 *
 * <pre>{@code mvnw -pl headway-ingest -am exec:java}</pre>
 */
public final class IngestMain {

    private static final Logger log = LoggerFactory.getLogger(IngestMain.class);

    public static void main(String[] args) throws Exception {
        GtfsRealtimeClient client = new GtfsRealtimeClient(GtfsRealtimeClient.MARTA_VEHICLE_POSITIONS);

        log.info("Fetching {}", client.feedUrl());
        FeedMessage feed = client.fetch();
        List<VehiclePosition> positions = GtfsRealtimeClient.toVehiclePositions(feed);

        Instant feedTime = Instant.ofEpochSecond(feed.getHeader().getTimestamp());
        log.info("GTFS-RT version {} | feed timestamp {} ({} old)",
                feed.getHeader().getGtfsRealtimeVersion(),
                feedTime,
                humanize(Duration.between(feedTime, Instant.now())));
        log.info("{} entities in feed -> {} usable vehicle positions",
                feed.getEntityCount(), positions.size());

        Map<String, Long> byRoute = positions.stream()
                .collect(Collectors.groupingBy(VehiclePosition::routeId, Collectors.counting()));

        log.info("{} routes currently have vehicles reporting. Busiest 10:", byRoute.size());
        byRoute.entrySet().stream()
                .sorted(Map.Entry.<String, Long>comparingByValue().reversed()
                        .thenComparing(Map.Entry.comparingByKey()))
                .limit(10)
                .forEach(e -> log.info("   route {} -> {} vehicles", e.getKey(), e.getValue()));

        log.info("Sample of 5 vehicles:");
        positions.stream()
                .sorted(Comparator.comparing(VehiclePosition::routeId)
                        .thenComparing(VehiclePosition::vehicleId))
                .limit(5)
                .forEach(vp -> log.info("   route {} | vehicle {} | ({}, {}) | {} | age {}",
                        vp.routeId(),
                        vp.vehicleId(),
                        "%.5f".formatted(vp.latitude()),
                        "%.5f".formatted(vp.longitude()),
                        vp.speed().isPresent() ? "%.1f m/s".formatted(vp.speed().getAsDouble()) : "no speed",
                        humanize(Duration.between(vp.timestamp(), Instant.now()))));
    }

    private static String humanize(Duration d) {
        long seconds = Math.max(0, d.toSeconds());
        return seconds < 60 ? seconds + "s" : (seconds / 60) + "m" + (seconds % 60) + "s";
    }

    private IngestMain() {}
}
