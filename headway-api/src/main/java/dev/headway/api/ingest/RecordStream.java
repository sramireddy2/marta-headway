package dev.headway.api.ingest;

import java.time.Duration;
import java.util.List;

/**
 * The three things this module actually needs from a Kafka consumer.
 *
 * <p>A narrow seam rather than passing {@code org.apache.kafka.clients.consumer.Consumer} around,
 * for two reasons.
 *
 * <p><b>It makes the threading contract structural.</b> {@code KafkaConsumer} is <em>not</em>
 * thread-safe, and the compiler will not stop you calling it from two threads — the class throws
 * {@code ConcurrentModificationException} at runtime if you try, which is the friendliest possible
 * version of a very unfriendly bug. The one exception is {@link #wakeup()}, which exists precisely
 * so another thread can interrupt a blocked {@link #poll}. Three methods, one of which is
 * documented as the cross-thread one, says that far more clearly than a comment on a forty-method
 * interface.
 *
 * <p><b>It makes the poll loop testable without a broker.</b> Kafka ships {@code MockConsumer}, but
 * its constructor signature has changed across major versions and it drags the whole consumer
 * lifecycle into a test that only wants to check "does a malformed record kill the loop".
 */
public interface RecordStream extends AutoCloseable {

    /** Begins consuming. Called once, from the polling thread, before the first poll. */
    void subscribe(String topic);

    /**
     * Waits up to {@code timeout} for records and returns their values as strings.
     *
     * <p>May only be called from the thread that owns this stream. Returns an empty list on
     * timeout, which is the normal case on a quiet topic.
     *
     * @throws org.apache.kafka.common.errors.WakeupException if {@link #wakeup()} was called
     */
    List<String> poll(Duration timeout);

    /**
     * Interrupts a blocked {@link #poll}. <b>The only method on this interface that may be called
     * from another thread.</b>
     */
    void wakeup();

    /** Releases the connection. Must be called from the owning thread. */
    @Override
    void close();
}
