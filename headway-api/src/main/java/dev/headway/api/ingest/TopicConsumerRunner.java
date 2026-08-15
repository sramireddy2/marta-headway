package dev.headway.api.ingest;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Polls one topic on one thread, parses each record, and hands it to a sink.
 *
 * <h2>The poll loop's two obligations</h2>
 *
 * <p><b>One bad record must not stop the stream.</b> If a single malformed message could throw out
 * of the loop, anyone able to write to the topic could halt the dashboard by producing the letter
 * {@code x}. Parsing and dispatch are therefore wrapped per record: the failure is counted, logged
 * once with the payload truncated, and the loop moves on. A rising {@link #malformedCount()} is the
 * signal that the producer and consumer have drifted apart — which is information, whereas a dead
 * thread is not.
 *
 * <p><b>Shutdown must not wait for a timeout.</b> {@code poll} blocks. Setting a flag and hoping is
 * how you get a five-second pause on every restart. {@link RecordStream#wakeup()} makes the blocked
 * poll throw {@link WakeupException} immediately, and the loop treats that as "we asked for this"
 * only if the flag is already down. A wakeup nobody requested is a real error and is rethrown,
 * because silently swallowing it would turn a bug into a mysteriously idle consumer.
 *
 * <h2>Why not a virtual thread</h2>
 *
 * Step 4 uses virtual threads for the HTTP fetch, where hundreds of tasks each spend their life
 * blocked. Here there is exactly one long-lived thread per topic that is never multiplexed with
 * anything. A virtual thread would add nothing, and it would <em>pin</em> its carrier for the whole
 * poll anyway. A named platform thread also shows up usefully in a stack dump.
 */
public final class TopicConsumerRunner<T> implements Runnable, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(TopicConsumerRunner.class);
    private static final Duration STOP_TIMEOUT = Duration.ofSeconds(5);

    private final String name;
    private final String topic;
    private final RecordStream stream;
    private final ObjectMapper mapper;
    private final Class<T> type;
    private final Consumer<T> sink;
    private final Duration pollTimeout;

    private final AtomicBoolean running = new AtomicBoolean(true);
    private final CountDownLatch finished = new CountDownLatch(1);

    private final LongAdder received = new LongAdder();
    private final LongAdder malformed = new LongAdder();
    private final AtomicReference<String> lastError = new AtomicReference<>();

    public TopicConsumerRunner(String name, String topic, RecordStream stream, ObjectMapper mapper,
            Class<T> type, Consumer<T> sink, Duration pollTimeout) {
        this.name = name;
        this.topic = topic;
        this.stream = stream;
        this.mapper = mapper;
        this.type = type;
        this.sink = sink;
        this.pollTimeout = pollTimeout;
    }

    @Override
    public void run() {
        log.info("{}: subscribing to '{}'", name, topic);
        try {
            stream.subscribe(topic);
            while (running.get()) {
                List<String> values = stream.poll(pollTimeout);
                for (String value : values) {
                    handle(value);
                }
            }
        } catch (WakeupException e) {
            if (running.get()) {
                throw e;
            }
            log.debug("{}: woken for shutdown", name);
        } catch (RuntimeException e) {
            // The thread is about to die, so this is the last chance to say why. Without it the
            // symptom is a dashboard that simply stops updating.
            lastError.set(e.toString());
            log.error("{}: consumer loop failed, no further records will be read", name, e);
            throw e;
        } finally {
            // close() must happen on the polling thread: the consumer is not thread-safe, and
            // closing it from the shutdown thread while this one is inside poll() is exactly the
            // concurrent access it forbids.
            stream.close();
            finished.countDown();
            log.info("{}: stopped after {} records ({} malformed)", name, received.sum(),
                    malformed.sum());
        }
    }

    private void handle(String value) {
        T parsed;
        try {
            parsed = mapper.readValue(value, type);
        } catch (Exception e) {
            malformed.increment();
            lastError.set(e.getMessage());
            if (malformed.sum() <= 5 || malformed.sum() % 100 == 0) {
                log.warn("{}: could not parse record ({} so far): {} | payload: {}",
                        name, malformed.sum(), e.getMessage(), truncate(value));
            }
            return;
        }
        try {
            sink.accept(parsed);
            received.increment();
        } catch (RuntimeException e) {
            // A sink that throws is a bug in our own code rather than bad input, but it still must
            // not take the stream down with it.
            lastError.set(e.toString());
            log.warn("{}: sink rejected a record", name, e);
        }
    }

    private static String truncate(String value) {
        if (value == null) {
            return "null";
        }
        return value.length() <= 200 ? value : value.substring(0, 200) + "...";
    }

    /**
     * Asks the loop to finish and waits briefly for it.
     *
     * <p>Safe to call from any thread, and safe to call twice — {@code compareAndSet} means only
     * the first caller issues the wakeup, and a second {@code wakeup()} on an already-closed
     * consumer would throw.
     */
    @Override
    public void close() {
        if (running.compareAndSet(true, false)) {
            stream.wakeup();
        }
        try {
            if (!finished.await(STOP_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)) {
                log.warn("{}: did not stop within {}", name, STOP_TIMEOUT);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public String name() {
        return name;
    }

    public long receivedCount() {
        return received.sum();
    }

    public long malformedCount() {
        return malformed.sum();
    }

    public String lastError() {
        return lastError.get();
    }

    public boolean isRunning() {
        return running.get() && finished.getCount() > 0;
    }
}
