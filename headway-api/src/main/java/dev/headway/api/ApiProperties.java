package dev.headway.api;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Everything about this service that someone might reasonably want to change without recompiling.
 *
 * <p>A record with {@code @DefaultValue} on each component rather than {@code @Value} fields: the
 * object is immutable and fully populated by the time any bean sees it, so there is no window in
 * which half the configuration is bound. The defaults live here rather than only in {@code
 * application.yml} so the class is correct even if that file is replaced.
 */
@ConfigurationProperties(prefix = "headway")
public record ApiProperties(

        /** Reachable from the host as localhost:9092; from inside Compose as kafka:29092. */
        @DefaultValue("localhost:9092") String bootstrapServers,

        @DefaultValue("route-headways") String headwayTopic,
        @DefaultValue("vehicle-positions") String vehicleTopic,

        /**
         * {@code latest} means the dashboard is blank until the stream job's next batch, up to 30
         * seconds. {@code earliest} fills it instantly but replays the topic's whole retention,
         * which for a week of data is a million records of work to reach the same state. Latest is
         * the right default for a live view; earliest is useful when demonstrating without the
         * stream job running.
         */
        @DefaultValue("latest") String startingOffsets,

        @DefaultValue("1s") Duration pollTimeout,

        /**
         * How long a route stays on the dashboard after its last measurement.
         *
         * <p>Must comfortably exceed the stream job's watermark, because these timestamps are
         * event time: window ends trail wall-clock by however late the GPS data was. Two minutes
         * of watermark plus a 30-second batch interval means anything under about five minutes
         * would evict routes that are working perfectly.
         */
        @DefaultValue("10m") Duration routeTtl,

        @DefaultValue("5m") Duration vehicleTtl,

        /** How long an alerting route may go silent before its episode is presumed over. */
        @DefaultValue("3m") Duration alertLinger,

        @DefaultValue("200") int alertHistory,

        /** How often the housekeeping sweep runs, in milliseconds. */
        @DefaultValue("15000") long sweepIntervalMillis,

        /**
         * How often a snapshot is pushed to connected browsers, in milliseconds.
         *
         * <p>Not "on every record". Updating a map 50 times a second is not 50 times more useful
         * than updating it once, and it would spend the entire budget serialising.
         */
        @DefaultValue("1000") long broadcastIntervalMillis,

        /** Per-socket send buffer before a slow client is disconnected. See the socket handler. */
        @DefaultValue("524288") int socketBufferBytes,

        @DefaultValue("10000") long socketSendTimeoutMillis) {
}
