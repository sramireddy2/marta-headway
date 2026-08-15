package dev.headway.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import dev.headway.api.ingest.TopicConsumerRunner;
import dev.headway.api.model.RouteHeadway;
import dev.headway.common.Json;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** The poll loop's two jobs: survive bad input, and stop when asked. */
@Timeout(30)
class TopicConsumerRunnerTest {

    private static final Duration POLL = Duration.ofMillis(50);
    private static final Instant NOW = Instant.parse("2026-08-15T14:00:00Z");

    private TopicConsumerRunner<RouteHeadway> runnerFor(QueueRecordStream stream,
            java.util.function.Consumer<RouteHeadway> sink) {
        return new TopicConsumerRunner<>("test", "route-headways", stream, Json.mapper(),
                RouteHeadway.class, sink, POLL);
    }

    @Test
    @DisplayName("records are parsed and handed to the sink in order")
    void happyPath() throws Exception {
        QueueRecordStream stream = new QueueRecordStream();
        CopyOnWriteArrayList<RouteHeadway> received = new CopyOnWriteArrayList<>();
        TopicConsumerRunner<RouteHeadway> runner = runnerFor(stream, received::add);

        Thread thread = new Thread(runner, "test-consumer");
        thread.start();
        try {
            stream.publish(Payloads.headway("10:1", NOW, "BUNCHING", 0.4, 500));
            stream.publish(Payloads.headway("51:0", NOW, "ON_SCHEDULE", 1.0, 5000));

            await().atMost(Duration.ofSeconds(5)).until(() -> received.size() == 2);

            assertThat(received).extracting(RouteHeadway::headwayGroup)
                    .containsExactly("10:1", "51:0");
            assertThat(stream.subscribedTopic()).isEqualTo("route-headways");
        } finally {
            runner.close();
            thread.join(5_000);
        }
    }

    /**
     * The important one. If one bad message could end the loop, anyone able to write to the topic
     * could stop the dashboard by producing a single character — and the symptom would be a
     * perfectly healthy-looking process serving stale data forever.
     */
    @Test
    @DisplayName("a malformed record is counted and skipped, and the loop keeps going")
    void malformedRecordDoesNotStopTheStream() throws Exception {
        QueueRecordStream stream = new QueueRecordStream();
        CopyOnWriteArrayList<RouteHeadway> received = new CopyOnWriteArrayList<>();
        TopicConsumerRunner<RouteHeadway> runner = runnerFor(stream, received::add);

        Thread thread = new Thread(runner, "test-consumer");
        thread.start();
        try {
            stream.publish("this is not json");
            stream.publish("{\"windowEnd\":\"yesterday\"}");
            stream.publish("");
            stream.publish(Payloads.headway("10:1", NOW, "BUNCHING", 0.4, 500));

            await().atMost(Duration.ofSeconds(5)).until(() -> received.size() == 1);

            assertThat(received.get(0).headwayGroup()).isEqualTo("10:1");
            assertThat(runner.malformedCount()).isEqualTo(3);
            assertThat(runner.receivedCount()).isEqualTo(1);
            assertThat(runner.isRunning()).isTrue();
        } finally {
            runner.close();
            thread.join(5_000);
        }
    }

    @Test
    @DisplayName("a sink that throws is logged, not fatal")
    void sinkFailureDoesNotStopTheStream() throws Exception {
        QueueRecordStream stream = new QueueRecordStream();
        AtomicInteger calls = new AtomicInteger();
        TopicConsumerRunner<RouteHeadway> runner = runnerFor(stream, headway -> {
            if (calls.incrementAndGet() == 1) {
                throw new IllegalStateException("deliberate");
            }
        });

        Thread thread = new Thread(runner, "test-consumer");
        thread.start();
        try {
            stream.publish(Payloads.headway("10:1", NOW, "BUNCHING", 0.4, 500));
            stream.publish(Payloads.headway("51:0", NOW, "BUNCHING", 0.4, 500));

            await().atMost(Duration.ofSeconds(5)).until(() -> calls.get() == 2);

            assertThat(runner.receivedCount())
                    .as("only the one the sink actually accepted").isEqualTo(1);
            assertThat(runner.isRunning()).isTrue();
        } finally {
            runner.close();
            thread.join(5_000);
        }
    }

    /**
     * {@code close()} must interrupt a blocked poll rather than waiting it out. This is checked
     * with a poll timeout far longer than the assertion window, so a runner that merely set a flag
     * and waited for the next poll to return would fail.
     */
    @Test
    @DisplayName("close() interrupts a blocked poll instead of waiting for it to time out")
    void shutdownIsPrompt() throws Exception {
        QueueRecordStream stream = new QueueRecordStream();
        TopicConsumerRunner<RouteHeadway> runner = new TopicConsumerRunner<>("test",
                "route-headways", stream, Json.mapper(), RouteHeadway.class, headway -> { },
                Duration.ofSeconds(30));

        Thread thread = new Thread(runner, "test-consumer");
        thread.start();
        await().atMost(Duration.ofSeconds(5)).until(() -> stream.pollCount() > 0);

        long before = System.nanoTime();
        runner.close();
        thread.join(5_000);
        long millis = (System.nanoTime() - before) / 1_000_000;

        assertThat(thread.isAlive()).isFalse();
        assertThat(millis).as("well under the 30-second poll timeout").isLessThan(3_000);
        assertThat(stream.isClosed())
                .as("the consumer is closed by the polling thread, not the caller").isTrue();
        assertThat(runner.isRunning()).isFalse();
    }

    @Test
    @DisplayName("close() twice is harmless")
    void closeIsIdempotent() throws Exception {
        QueueRecordStream stream = new QueueRecordStream();
        TopicConsumerRunner<RouteHeadway> runner = runnerFor(stream, headway -> { });

        Thread thread = new Thread(runner, "test-consumer");
        thread.start();
        await().atMost(Duration.ofSeconds(5)).until(() -> stream.pollCount() > 0);

        runner.close();
        runner.close();
        thread.join(5_000);

        assertThat(thread.isAlive()).isFalse();
    }

    @Test
    @DisplayName("the sink sees every record from a burst, not just the first")
    void drainsWholeBatches() throws Exception {
        QueueRecordStream stream = new QueueRecordStream();
        CopyOnWriteArrayList<RouteHeadway> received = new CopyOnWriteArrayList<>();
        TopicConsumerRunner<RouteHeadway> runner = runnerFor(stream, received::add);

        Thread thread = new Thread(runner, "test-consumer");
        thread.start();
        try {
            List<String> burst = new java.util.ArrayList<>();
            for (int i = 0; i < 50; i++) {
                burst.add(Payloads.headway(i + ":0", NOW, "ON_SCHEDULE", 1.0, 5000));
            }
            burst.forEach(stream::publish);

            await().atMost(Duration.ofSeconds(5)).until(() -> received.size() == 50);
            assertThat(runner.malformedCount()).isZero();
        } finally {
            runner.close();
            thread.join(5_000);
        }
    }
}
