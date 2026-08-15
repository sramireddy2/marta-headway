package dev.headway.ingest.kafka;

import dev.headway.common.VehiclePosition;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Publishes decoded vehicle positions to Kafka, keyed by route.
 *
 * <p>This slots into the {@code downstream} seam that {@link dev.headway.ingest.FeedPoller} has
 * had since step 2, so adding Kafka changed one line of wiring rather than the poller.
 *
 * <h2>Why the key is the route id</h2>
 *
 * A Kafka topic is split into <b>partitions</b>. Kafka guarantees ordering <em>within</em> a
 * partition and makes no promise at all <em>across</em> partitions. When a record has a key, the
 * producer hashes it and sends every record with that key to the same partition.
 *
 * <p>Headway compares buses on the same route against each other. If route 15's updates were
 * spread over six partitions, a consumer could read 10:00:30 for one bus before 10:00:15 for the
 * bus in front of it and compute a gap from readings that never coexisted. Keying by
 * {@code routeId} puts one route's entire history in one partition, in order, forever.
 *
 * <p>Keying by {@code vehicleId} would be the tempting mistake. It spreads load more evenly — but
 * it scatters a single route across every partition and destroys exactly the ordering the headway
 * calculation depends on. The right key is dictated by the query you intend to run, not by load
 * balancing.
 *
 * <p>The cost is a hot partition when one route is much busier than the rest. With ~65 routes over
 * 6 partitions that is a non-issue, and correctness is not negotiable for a few percent of skew.
 */
public final class KafkaPositionPublisher implements Consumer<VehiclePosition>, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(KafkaPositionPublisher.class);

    public static final String DEFAULT_TOPIC = "vehicle-positions";
    public static final String DEFAULT_BOOTSTRAP = "localhost:9092";
    public static final int DEFAULT_PARTITIONS = 6;

    private final Producer<String, VehiclePosition> producer;
    private final String topic;

    private final LongAdder sent = new LongAdder();
    private final LongAdder failed = new LongAdder();

    // Set via withMetrics(). Called from the producer's I/O thread, so they must be cheap and
    // must not throw — a Micrometer counter increment is both.
    private volatile Runnable onSuccess;
    private volatile Runnable onFailure;

    /** Primary constructor. Takes a {@link Producer} so tests can pass a {@code MockProducer}. */
    public KafkaPositionPublisher(Producer<String, VehiclePosition> producer, String topic) {
        this.producer = producer;
        this.topic = topic;
    }

    /** Builds a real producer against a running broker. */
    public static KafkaPositionPublisher create(String bootstrapServers, String topic) {
        return new KafkaPositionPublisher(new KafkaProducer<>(producerConfig(bootstrapServers)), topic);
    }

    static Properties producerConfig(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "headway-ingest");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, VehiclePositionSerializer.class.getName());

        // Wait for all in-sync replicas to confirm the write, not just the partition leader.
        // With acks=1 a leader can acknowledge and then die before any follower has the record,
        // and the write is silently lost. Locally there is one replica so this costs nothing;
        // setting it now means the config is already right when it stops being free.
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        // The producer retries on transient errors. Without idempotence a retry can duplicate a
        // record (the write succeeded, the acknowledgement was lost). Idempotence has the broker
        // deduplicate by sequence number, so a retry is a no-op. This is the same property the
        // VehicleStore enforces in memory, now enforced on the wire.
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        // Wait up to 20ms to fill a batch before sending. A tiny latency cost buys a large
        // throughput win, because ~190 records per poll then travel as a handful of requests
        // instead of 190 round trips.
        props.put(ProducerConfig.LINGER_MS_CONFIG, 20);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");

        // Default is 60s. If the broker is down, send() blocks that long waiting for metadata,
        // which looks exactly like a hang. Ten seconds fails fast with a comprehensible error.
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 10_000);
        return props;
    }

    /**
     * Creates the topic if it does not exist.
     *
     * <p>Brokers can auto-create topics, but they do it with the broker's default partition count
     * and replication factor, which is how you end up with a one-partition topic in production and
     * no idea why throughput is capped. Declaring it explicitly makes the choice visible and
     * reviewable.
     */
    public static void ensureTopic(String bootstrapServers, String topic, int partitions) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("default.api.timeout.ms", 15_000);
        props.put("request.timeout.ms", 10_000);

        try (Admin admin = Admin.create(props)) {
            NewTopic newTopic = new NewTopic(topic, partitions, (short) 1);
            admin.createTopics(List.of(newTopic)).all().get(20, TimeUnit.SECONDS);
            log.info("Created topic '{}' with {} partitions", topic, partitions);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof TopicExistsException) {
                // Expected on every run after the first. Not an error.
                log.info("Topic '{}' already exists", topic);
            } else {
                throw new IllegalStateException("Could not create topic " + topic, e.getCause());
            }
        } catch (TimeoutException e) {
            throw new IllegalStateException(
                    "Timed out talking to Kafka at " + bootstrapServers + ". Is `docker compose up -d` running?", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while creating topic " + topic, e);
        }
    }

    /**
     * Publishes one position.
     *
     * <p>{@code send()} is asynchronous: it appends to an in-memory batch and returns immediately,
     * while a background I/O thread does the network work. Calling {@code .get()} on the returned
     * future per record would make it synchronous and collapse throughput by orders of magnitude —
     * a very common mistake. Instead we pass a callback and let the batch fly.
     *
     * <p>Called concurrently by one worker per shard. {@link KafkaProducer} is thread-safe and is
     * designed to be shared; one producer per thread would multiply the buffers and the
     * connections for no gain. Per-route ordering survives because a route only ever reaches one
     * worker, so its records are always offered to the producer in sequence.
     */
    @Override
    public void accept(VehiclePosition position) {
        ProducerRecord<String, VehiclePosition> record =
                new ProducerRecord<>(topic, position.routeId(), position);

        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                failed.increment();
                if (onFailure != null) {
                    onFailure.run();
                }
                log.error("Failed to publish vehicle {} on route {}",
                        position.vehicleId(), position.routeId(), exception);
            } else {
                sent.increment();
                if (onSuccess != null) {
                    onSuccess.run();
                }
            }
        });
    }

    /** Convenience for callers that still hold a batch (tests, and the odd bulk path). */
    public void acceptAll(List<VehiclePosition> positions) {
        positions.forEach(this::accept);
    }

    /**
     * Hooks so Micrometer counters move when the broker acknowledges, rather than when we asked.
     * A record is only really published once the callback fires.
     */
    public KafkaPositionPublisher withMetrics(Runnable onSuccess, Runnable onFailure) {
        this.onSuccess = onSuccess;
        this.onFailure = onFailure;
        return this;
    }

    public long sentTotal() {
        return sent.sum();
    }

    public long failedTotal() {
        return failed.sum();
    }

    /**
     * Flushes anything still batched, then closes.
     *
     * <p>Without the flush, records sitting in the {@code linger.ms} buffer when the JVM exits are
     * simply lost. {@code close()} does flush internally, but calling it explicitly makes the
     * intent obvious and lets us log the final tally.
     */
    @Override
    public void close() {
        producer.flush();
        log.info("Kafka publisher closing: {} sent, {} failed", sentTotal(), failedTotal());
        producer.close(Duration.ofSeconds(10));
    }

    /** Reads config from the environment so the app runs unchanged inside a container later. */
    public static Map<String, String> environmentConfig() {
        String bootstrap = System.getenv().getOrDefault("HEADWAY_KAFKA_BOOTSTRAP", DEFAULT_BOOTSTRAP);
        String topic = System.getenv().getOrDefault("HEADWAY_KAFKA_TOPIC", DEFAULT_TOPIC);
        return Map.of("bootstrap", bootstrap, "topic", topic);
    }
}
