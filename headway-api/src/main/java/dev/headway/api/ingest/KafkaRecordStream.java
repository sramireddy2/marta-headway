package dev.headway.api.ingest;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

/** {@link RecordStream} backed by a real {@link KafkaConsumer}. */
public final class KafkaRecordStream implements RecordStream {

    private final KafkaConsumer<String, String> consumer;

    /**
     * @param groupPrefix a label like {@code headway-api-headways}; a unique suffix is appended
     * @param startingOffsets {@code latest} or {@code earliest}
     */
    public KafkaRecordStream(String bootstrapServers, String groupPrefix, String startingOffsets) {
        Properties config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        // A UNIQUE GROUP ID PER PROCESS, AND THIS IS NOT A DETAIL.
        //
        // A consumer group divides a topic's partitions between its members so that each record is
        // handled once - the right model for workers sharing a queue. This is the opposite case:
        // every API instance needs *every* record, because each one serves a complete dashboard.
        // Share a group id between two instances and Kafka will hand each of them half the
        // partitions, so each shows roughly half the network's routes and neither looks broken.
        // That is a miserable thing to debug, and a UUID prevents it entirely.
        config.put(ConsumerConfig.GROUP_ID_CONFIG,
                groupPrefix + "-" + UUID.randomUUID().toString().substring(0, 8));

        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, startingOffsets);

        // Nothing to commit. The dashboard's state is a projection of the newest records, rebuilt
        // from scratch on every start; a committed offset would only let a restart resume from
        // somewhere stale and briefly display history as though it were live.
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        // Bounds how much one poll can return, which bounds how long the loop goes without
        // checking the running flag on shutdown.
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 2000);

        this.consumer = new KafkaConsumer<>(config);
    }

    @Override
    public void subscribe(String topic) {
        consumer.subscribe(List.of(topic));
    }

    @Override
    public List<String> poll(Duration timeout) {
        ConsumerRecords<String, String> records = consumer.poll(timeout);
        List<String> values = new ArrayList<>(records.count());
        for (ConsumerRecord<String, String> record : records) {
            if (record.value() != null) {
                values.add(record.value());
            }
        }
        return values;
    }

    @Override
    public void wakeup() {
        consumer.wakeup();
    }

    @Override
    public void close() {
        consumer.close();
    }
}
