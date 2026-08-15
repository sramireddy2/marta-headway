package dev.headway.api.web;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.headway.api.ApiProperties;
import dev.headway.api.ingest.KafkaIngestService;
import dev.headway.api.ingest.TopicConsumerRunner;
import dev.headway.api.model.LiveSnapshot;
import dev.headway.api.model.ServiceStatus;
import dev.headway.api.state.AlertTracker;
import dev.headway.api.state.LiveHeadwayState;
import dev.headway.api.state.VehicleState;
import dev.headway.common.Json;
import jakarta.annotation.PreDestroy;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

/**
 * Builds the snapshot the dashboard sees, and pushes it to browsers on a fixed tick.
 *
 * <h2>Why a timer rather than pushing on every record</h2>
 *
 * Records arrive in bursts — one Spark batch delivers fifty route updates in a few milliseconds.
 * Broadcasting per record would send fifty near-identical frames, forty-nine of which are obsolete
 * before they leave the building. A one-second tick sends one frame containing all fifty changes,
 * which is both less work and more accurate: a frame assembled mid-burst shows half a batch
 * applied, which is a state the system was never really in.
 *
 * <p>The tick then goes through a {@link Conflator} rather than sending inline, so that a slow
 * broadcast cannot make the scheduler thread late for the housekeeping sweep, and so that ticks
 * cannot pile up behind one another if serialisation ever gets slower than the interval.
 */
@Service
public final class SnapshotBroadcaster {

    private static final Logger log = LoggerFactory.getLogger(SnapshotBroadcaster.class);

    private final ApiProperties properties;
    private final LiveHeadwayState headways;
    private final VehicleState vehicles;
    private final AlertTracker alerts;
    private final KafkaIngestService ingest;
    private final LiveSocketHandler sockets;
    private final ObjectMapper mapper = Json.mapper();

    private final AtomicLong sequence = new AtomicLong();
    private final Conflator<LiveSnapshot> broadcaster;

    public SnapshotBroadcaster(ApiProperties properties, LiveHeadwayState headways,
            VehicleState vehicles, AlertTracker alerts, KafkaIngestService ingest,
            LiveSocketHandler sockets) {
        this.properties = properties;
        this.headways = headways;
        this.vehicles = vehicles;
        this.alerts = alerts;
        this.ingest = ingest;
        this.sockets = sockets;
        this.broadcaster = new Conflator<>("headway-broadcast", this::serialiseAndSend);

        // One-way wiring: this bean knows the socket handler, and hands it a way to greet a new
        // connection with the current state. Injecting this bean into the handler instead would
        // make a cycle out of what is really a callback.
        sockets.onInitialFrame(this::currentFrame);
    }

    /** Everything a client needs, as of now. */
    public LiveSnapshot snapshot() {
        return new LiveSnapshot(
                Instant.now(),
                sequence.incrementAndGet(),
                headways.current(),
                alerts.openEpisodes(),
                List.copyOf(vehicles.all()),
                status());
    }

    public ServiceStatus status() {
        List<ServiceStatus.TopicStatus> topics = ingest.runners().stream()
                .map(runner -> new ServiceStatus.TopicStatus(
                        runner.name(), runner.isRunning(), runner.receivedCount(),
                        runner.malformedCount(), runner.lastError()))
                .toList();

        long headwayRecords = named(topics, "headways", ServiceStatus.TopicStatus::received);
        long vehicleRecords = named(topics, "vehicles", ServiceStatus.TopicStatus::received);
        long malformed = topics.stream().mapToLong(ServiceStatus.TopicStatus::malformed).sum();

        return new ServiceStatus(
                Instant.now(),
                ingest.isRunning(),
                properties.bootstrapServers(),
                topics,
                headways.size(),
                vehicles.size(),
                alerts.openCount(),
                headwayRecords,
                vehicleRecords,
                malformed,
                headways.staleCount(),
                alerts.openedTotal(),
                alerts.absorbedTotal(),
                alerts.clearedTotal(),
                broadcaster.deliveredCount(),
                broadcaster.coalescedCount(),
                sockets.connectionCount(),
                headways.newestWindowEnd().orElse(null));
    }

    /** Pushes the current state to every connected browser. */
    @Scheduled(fixedDelayString = "${headway.broadcast-interval-millis:1000}")
    public void tick() {
        if (sockets.connectionCount() == 0) {
            return;      // nobody is listening; building the frame would be pure waste
        }
        broadcaster.submit(snapshot());
    }

    private void serialiseAndSend(LiveSnapshot snapshot) {
        try {
            // Serialised once here, then the same string goes to every session.
            sockets.broadcast(mapper.writeValueAsString(snapshot));
        } catch (Exception e) {
            log.warn("Could not serialise snapshot {}", snapshot.sequence(), e);
        }
    }

    private String currentFrame() {
        try {
            return mapper.writeValueAsString(snapshot());
        } catch (Exception e) {
            log.warn("Could not build the initial frame", e);
            return null;
        }
    }

    private static long named(List<ServiceStatus.TopicStatus> topics, String name,
            java.util.function.ToLongFunction<ServiceStatus.TopicStatus> field) {
        return topics.stream()
                .filter(topic -> topic.name().equals(name))
                .mapToLong(field)
                .findFirst()
                .orElse(0);
    }

    @PreDestroy
    void shutdown() {
        broadcaster.close();
    }
}
