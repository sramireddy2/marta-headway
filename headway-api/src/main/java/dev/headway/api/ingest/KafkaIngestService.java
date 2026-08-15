package dev.headway.api.ingest;

import dev.headway.api.ApiProperties;
import dev.headway.api.model.RouteHeadway;
import dev.headway.api.state.AlertTracker;
import dev.headway.api.state.LiveHeadwayState;
import dev.headway.api.state.VehicleState;
import dev.headway.common.Json;
import dev.headway.common.VehiclePosition;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

/**
 * Owns the two consumer threads and points them at the in-memory state.
 *
 * <pre>
 *   route-headways   --&gt; LiveHeadwayState  (current headway per route:direction)
 *                     \-&gt; AlertTracker      (repeated windows collapsed into episodes)
 *   vehicle-positions --&gt; VehicleState      (last known position per bus, for the map)
 * </pre>
 *
 * <h2>Two topics, two threads, and no lock between them</h2>
 *
 * A Kafka consumer must not be shared between threads, so each topic gets its own. They then write
 * to different {@code ConcurrentHashMap}s and never touch each other's state, which is why there is
 * no synchronisation here at all. It is worth noticing when a design has no locks because it does
 * not need any, rather than because someone forgot.
 *
 * <h2>Why {@link SmartLifecycle} and not {@code @PostConstruct}</h2>
 *
 * {@code @PostConstruct} runs while the context is still being built. Starting a thread there means
 * records can arrive and call into beans that are not fully initialised yet. {@code SmartLifecycle}
 * runs after refresh completes and, more importantly, gives an ordered {@code stop()} before the
 * beans are destroyed — so the consumers are shut down while the state they write to still exists.
 *
 * <p>The service starts whether or not Kafka is reachable. A broker that is down produces warnings
 * and empty polls, and the consumer reconnects on its own when it comes back. Failing startup
 * instead would mean the API cannot serve {@code /api/status} to tell you Kafka is down, which is
 * the one moment you most want it.
 */
@Component
public final class KafkaIngestService implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(KafkaIngestService.class);

    private final ApiProperties properties;
    private final RecordStreamFactory streams;
    private final LiveHeadwayState headways;
    private final AlertTracker alerts;
    private final VehicleState vehicles;

    private final List<TopicConsumerRunner<?>> runners = new ArrayList<>();
    private final List<Thread> threads = new ArrayList<>();
    private volatile boolean running;

    public KafkaIngestService(ApiProperties properties, RecordStreamFactory streams,
            LiveHeadwayState headways, AlertTracker alerts, VehicleState vehicles) {
        this.properties = properties;
        this.streams = streams;
        this.headways = headways;
        this.alerts = alerts;
        this.vehicles = vehicles;
    }

    @Override
    public synchronized void start() {
        if (running) {
            return;
        }
        runners.add(new TopicConsumerRunner<>(
                "headways", properties.headwayTopic(), streams.create("headway-api-headways"),
                Json.mapper(), RouteHeadway.class, this::onHeadway, properties.pollTimeout()));

        runners.add(new TopicConsumerRunner<>(
                "vehicles", properties.vehicleTopic(), streams.create("headway-api-vehicles"),
                Json.mapper(), VehiclePosition.class, vehicles::accept, properties.pollTimeout()));

        for (TopicConsumerRunner<?> runner : runners) {
            Thread thread = new Thread(runner, "headway-consumer-" + runner.name());
            // Not a daemon. A daemon thread would be killed mid-poll on JVM exit; this way
            // SmartLifecycle.stop() gets to close the consumer cleanly, which lets the broker
            // release the group membership immediately instead of after a session timeout.
            thread.setDaemon(false);
            threads.add(thread);
            thread.start();
        }
        running = true;
        log.info("Consuming '{}' and '{}' from {}", properties.headwayTopic(),
                properties.vehicleTopic(), properties.bootstrapServers());
    }

    /**
     * One measurement updates two views of the world.
     *
     * <p>Order matters: the alert tracker is fed only if this record is actually the newest for its
     * group. A late-arriving older window that {@link LiveHeadwayState} rejected must not be able
     * to reopen an episode that has already cleared.
     */
    private void onHeadway(RouteHeadway headway) {
        if (!headways.accept(headway)) {
            return;
        }
        AlertTracker.Observation observation = alerts.observe(headway);
        if (observation.isNotable()) {
            log.info("{} {} on {} - {} buses, gap {} m against an expected {} m (ratio {})",
                    observation.change(),
                    observation.episode().worstStatus(),
                    headway.displayName(),
                    headway.vehicleCount(),
                    format(headway.minGapMetres()),
                    format(headway.expectedSpacingMetres()),
                    format(headway.headwayRatio()));
        }
    }

    private static String format(Double value) {
        return value == null ? "?" : String.format("%.1f", value);
    }

    @Override
    public synchronized void stop() {
        if (!running) {
            return;
        }
        running = false;
        // Ask every consumer to stop before waiting on any of them, so the shutdown costs one
        // poll timeout in total rather than one per topic.
        runners.forEach(TopicConsumerRunner::close);
        for (Thread thread : threads) {
            try {
                thread.join(5_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
        runners.clear();
        threads.clear();
        log.info("Consumers stopped");
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    /** Runs late on start and early on stop, so consumers exist only while the web layer does. */
    @Override
    public int getPhase() {
        return Integer.MAX_VALUE - 1000;
    }

    public List<TopicConsumerRunner<?>> runners() {
        return List.copyOf(runners);
    }
}
