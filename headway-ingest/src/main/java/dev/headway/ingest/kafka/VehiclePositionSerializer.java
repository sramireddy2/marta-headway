package dev.headway.ingest.kafka;

import dev.headway.common.Json;
import dev.headway.common.VehiclePosition;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Turns a {@link VehiclePosition} into the JSON bytes that go on the wire.
 *
 * <p>Kafka itself has no opinion about your data: a message is a key byte-array and a value
 * byte-array. A {@link Serializer} is the adapter between your types and those bytes, and the
 * producer calls it for every record.
 *
 * <p>JSON is not the most compact choice — Avro or Protobuf with a schema registry would be
 * smaller and would enforce schema evolution. JSON is chosen here because you can read a message
 * with {@code kafka-console-consumer} and immediately see whether it is right, which matters far
 * more while you are building the thing than a few bytes per record do.
 */
public final class VehiclePositionSerializer implements Serializer<VehiclePosition> {

    @Override
    public byte[] serialize(String topic, VehiclePosition data) {
        // Kafka uses a null value as a tombstone (a delete marker in a compacted topic), so a null
        // input must produce a null output rather than the four bytes spelling "null".
        return data == null ? null : Json.toBytes(data);
    }
}
