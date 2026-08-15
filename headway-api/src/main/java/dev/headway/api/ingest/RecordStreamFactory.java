package dev.headway.api.ingest;

/**
 * Creates a {@link RecordStream} for one logical consumer.
 *
 * <p>A bean rather than a {@code new} inside the service, so a test can stand up the entire Spring
 * context — controllers, WebSocket handler, scheduled sweeps and all — with fakes in place of
 * brokers. That is the difference between testing the wiring and testing the classes.
 *
 * @see dev.headway.api.ingest.KafkaRecordStream
 */
@FunctionalInterface
public interface RecordStreamFactory {

    /** @param groupPrefix a human-readable label; the implementation makes the group id unique */
    RecordStream create(String groupPrefix);
}
