package dev.headway.common;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * The single JSON configuration used everywhere in Headway.
 *
 * <p>Jackson turns Java objects into JSON bytes and back. Spring Boot will wire it up for you in
 * step 9, but here we configure it by hand because the exact JSON shape is a <em>contract</em>:
 * the ingest service writes it, and Spark and the API read it, possibly after a redeploy. Two
 * differently-configured mappers in the same system is a bug waiting to happen.
 *
 * <p>{@link ObjectMapper} is expensive to build and <b>thread-safe once configured</b>, so the
 * right pattern is exactly one shared static instance. Creating one per message is a real and
 * common performance bug. It is only unsafe to <em>reconfigure</em> a mapper after sharing it,
 * which is why the shared instance here is never handed out for mutation.
 */
public final class Json {

    private static final ObjectMapper MAPPER = newMapper();

    /** Builds a mapper configured the Headway way. Prefer {@link #mapper()} unless you must own one. */
    public static ObjectMapper newMapper() {
        return new ObjectMapper()
                // Instant has no meaning to core Jackson; this module teaches it java.time.
                .registerModule(new JavaTimeModule())

                // Off by default, Jackson writes Instants as a float like 1755207819.123456789.
                // Turning it off gives ISO-8601: "2026-08-14T21:43:39Z". Slightly larger on the
                // wire, but readable in kafka-console-consumer, sortable as a string, and
                // parseable by Spark without a custom UDF. Worth every byte.
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)

                // Forward compatibility. When a later step adds a field to VehiclePosition, an
                // older consumer that has not been redeployed must skip it rather than crash.
                // Without this, adding one field breaks every consumer at once.
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    }

    /** The shared, thread-safe mapper. Do not reconfigure it. */
    public static ObjectMapper mapper() {
        return MAPPER;
    }

    /**
     * Serialises to UTF-8 JSON bytes.
     *
     * <p>Wraps Jackson's checked {@link IOException} in an unchecked one. Serialising an in-memory
     * object cannot fail for I/O reasons — if it throws, the object graph is wrong, which is a
     * programming error, not a condition a caller can sensibly recover from.
     */
    public static byte[] toBytes(Object value) {
        try {
            return MAPPER.writeValueAsBytes(value);
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot serialise " + value.getClass().getName(), e);
        }
    }

    public static String toString(Object value) {
        return new String(toBytes(value), java.nio.charset.StandardCharsets.UTF_8);
    }

    /**
     * Parses UTF-8 JSON bytes.
     *
     * <p>This one really can fail on bad input — a truncated or corrupt message from the network —
     * so callers are expected to handle it.
     */
    public static <T> T fromBytes(byte[] bytes, Class<T> type) throws IOException {
        return MAPPER.readValue(bytes, type);
    }

    private Json() {}
}
