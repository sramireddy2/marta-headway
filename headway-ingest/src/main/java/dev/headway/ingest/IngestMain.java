package dev.headway.ingest;

import dev.headway.common.VehiclePosition;
import dev.headway.ingest.kafka.KafkaPositionPublisher;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Step 3 entry point: poll MARTA continuously and publish every position to Kafka.
 *
 * <p>Requires a broker. Start one with {@code docker compose up -d}.
 *
 * <pre>{@code .\mvnw.cmd -q -pl headway-ingest -am package exec:java -DskipTests}</pre>
 *
 * <p>Press Ctrl+C to stop; the shutdown hook drains the executor and flushes Kafka.
 */
public final class IngestMain {

    private static final Logger log = LoggerFactory.getLogger(IngestMain.class);

    public static void main(String[] args) throws Exception {
        Map<String, String> kafka = KafkaPositionPublisher.environmentConfig();
        String bootstrap = kafka.get("bootstrap");
        String topic = kafka.get("topic");

        log.info("Kafka at {}, topic '{}'", bootstrap, topic);
        KafkaPositionPublisher.ensureTopic(bootstrap, topic, KafkaPositionPublisher.DEFAULT_PARTITIONS);

        GtfsRealtimeClient client = new GtfsRealtimeClient(GtfsRealtimeClient.MARTA_VEHICLE_POSITIONS);

        // The `downstream` seam from step 2 finally earns its keep: swapping a debug log for a
        // Kafka producer is this one line.
        try (KafkaPositionPublisher publisher = KafkaPositionPublisher.create(bootstrap, topic)) {
            IngestService service =
                    new IngestService(client, IngestService.Config.defaults(), publisher);

            // A shutdown hook runs when the JVM is asked to exit: Ctrl+C, `kill`, IDE stop button.
            // Order matters here — stop polling first, then flush Kafka, so nothing is still being
            // produced while we are trying to drain.
            CountDownLatch shutdown = new CountDownLatch(1);
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("Shutdown signal received.");
                service.close();
                publisher.close();
                printBusiestRoutes(service.store());
                shutdown.countDown();
            }, "headway-shutdown"));

            service.start();
            log.info("Ingest running. Ctrl+C to stop.");
            shutdown.await();
        }
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
