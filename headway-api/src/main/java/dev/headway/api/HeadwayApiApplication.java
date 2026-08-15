package dev.headway.api;

import dev.headway.api.ingest.KafkaRecordStream;
import dev.headway.api.ingest.RecordStreamFactory;
import dev.headway.api.state.AlertTracker;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * The front door: REST at {@code /api/**}, a live feed at {@code /ws/live}.
 *
 * <pre>
 *   Kafka route-headways ─┐
 *                         ├─► one thread each ─► ConcurrentHashMaps ─┬─► REST  (ask)
 *   Kafka vehicle-positions ┘                                        └─► WebSocket (listen)
 * </pre>
 *
 * <p>Nothing in this module computes a headway. Everything it serves was already decided by the
 * Spark job in step 8; the value added here is <em>shape</em>. A topic is a log of measurements,
 * and a dashboard needs the current state — those are different data structures, and turning one
 * into the other is this module's whole job.
 *
 * <p>Two consequences fall out of that framing:
 *
 * <ul>
 *   <li>All state is a projection and can be rebuilt by re-reading the topic, so nothing is
 *       persisted and a restart costs one batch interval.
 *   <li>Repeated measurements of one event are a property of the log, not of reality, so
 *       collapsing them belongs here. That is {@code AlertTracker}, and it is the fix for the
 *       duplicate-alert limitation step 8 documented.
 * </ul>
 *
 * <p>Run it with {@code mvn -pl headway-api -am spring-boot:run}, or against a broker inside
 * Compose with {@code HEADWAY_BOOTSTRAP_SERVERS=kafka:29092}.
 */
@SpringBootApplication
@EnableConfigurationProperties(ApiProperties.class)
@EnableScheduling
public class HeadwayApiApplication {

    public static void main(String[] args) {
        SpringApplication.run(HeadwayApiApplication.class, args);
    }

    /**
     * The real Kafka-backed stream factory.
     *
     * <p>A {@code @Bean} rather than a {@code new} inside the ingest service purely so tests can
     * replace it — a test declares its own factory as {@code @Primary} and the whole context comes
     * up with fakes at the only point that touches a network.
     */
    @Bean
    RecordStreamFactory recordStreamFactory(ApiProperties properties) {
        return groupPrefix -> new KafkaRecordStream(
                properties.bootstrapServers(), groupPrefix, properties.startingOffsets());
    }

    /** The alert history bound is configuration, so the tracker cannot be a plain @Component. */
    @Bean
    AlertTracker alertTracker(ApiProperties properties) {
        return new AlertTracker(properties.alertHistory());
    }
}
