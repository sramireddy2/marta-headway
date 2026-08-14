package dev.headway.ingest;

import dev.headway.common.VehiclePosition;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Step 2 entry point: poll MARTA continuously and keep a live view of every bus in memory.
 *
 * <p>Run with:
 *
 * <pre>{@code mvnw -q -pl headway-ingest -am package exec:java -DskipTests}</pre>
 *
 * <p>Press Ctrl+C to stop; the shutdown hook drains the executor cleanly.
 */
public final class IngestMain {

    private static final Logger log = LoggerFactory.getLogger(IngestMain.class);

    public static void main(String[] args) throws Exception {
        GtfsRealtimeClient client = new GtfsRealtimeClient(GtfsRealtimeClient.MARTA_VEHICLE_POSITIONS);

        // In step 3 this lambda becomes "publish to Kafka". Wiring the seam in now means that
        // change touches one line instead of restructuring the poller.
        Consumer<List<VehiclePosition>> downstream =
                positions -> log.debug("downstream received {} positions", positions.size());

        IngestService service =
                new IngestService(client, IngestService.Config.defaults(), downstream);

        // A shutdown hook runs when the JVM is asked to exit: Ctrl+C, `kill`, IDE stop button.
        // Without one, Ctrl+C kills the process mid-poll and (later) leaves Kafka messages
        // unflushed. The latch lets main() park until then instead of spinning.
        CountDownLatch shutdown = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown signal received.");
            service.close();
            printBusiestRoutes(service.store());
            shutdown.countDown();
        }, "headway-shutdown"));

        service.start();
        log.info("Ingest running. Ctrl+C to stop.");
        shutdown.await();
    }

    /**
     * Routes with the most vehicles are the ones most likely to be bunching. This is a crude
     * preview of what step 7 computes properly, using distance along the route rather than a count.
     */
    private static void printBusiestRoutes(VehicleStore store) {
        Map<String, Long> byRoute = store.snapshot().values().stream()
                .collect(Collectors.groupingBy(VehiclePosition::routeId, Collectors.counting()));

        log.info("Final tally: {} vehicles across {} routes", store.size(), byRoute.size());
        byRoute.entrySet().stream()
                .sorted(Map.Entry.<String, Long>comparingByValue().reversed()
                        .thenComparing(Map.Entry.comparingByKey()))
                .limit(5)
                .forEach(e -> log.info("   route {} -> {} vehicles", e.getKey(), e.getValue()));
    }

    private IngestMain() {}
}
