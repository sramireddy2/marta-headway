package dev.headway.common;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.time.Instant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class JsonTest {

    private static final Instant T0 = Instant.parse("2026-08-14T21:00:00Z");

    private static VehiclePosition sample() {
        return new VehiclePosition("bus-1", "15", "trip-9", 0,
                33.75432, -84.39001, 91.5f, 12.25f, T0);
    }

    @Test
    @DisplayName("a VehiclePosition survives a round trip unchanged")
    void roundTrips() throws IOException {
        VehiclePosition original = sample();

        VehiclePosition parsed = Json.fromBytes(Json.toBytes(original), VehiclePosition.class);

        assertThat(parsed).isEqualTo(original);
    }

    @Test
    @DisplayName("optional fields survive as null rather than becoming zero")
    void roundTripsWithNulls() throws IOException {
        VehiclePosition sparse = new VehiclePosition("bus-2", "110", null, null,
                33.7, -84.4, null, null, T0);

        VehiclePosition parsed = Json.fromBytes(Json.toBytes(sparse), VehiclePosition.class);

        assertThat(parsed).isEqualTo(sparse);
        assertThat(parsed.tripId()).isNull();
        assertThat(parsed.speed()).isEmpty();
    }

    /**
     * The wire format is a contract with Spark and the API, so it is worth asserting directly
     * rather than trusting Jackson's defaults not to change under a version bump.
     */
    @Test
    @DisplayName("timestamps are ISO-8601 strings, not float epoch seconds")
    void timestampsAreReadable() {
        String json = Json.toString(sample());

        assertThat(json).contains("\"timestamp\":\"2026-08-14T21:00:00Z\"");
        assertThat(json).doesNotContain("1755205200");
    }

    @Test
    @DisplayName("an unknown field from a newer producer is skipped, not fatal")
    void toleratesUnknownFields() throws IOException {
        String fromTheFuture = """
                {"vehicleId":"bus-1","routeId":"15","tripId":"trip-9","directionId":0,
                 "latitude":33.75,"longitude":-84.39,"bearingDegrees":null,
                 "speedMetersPerSecond":null,"timestamp":"2026-08-14T21:00:00Z",
                 "occupancyStatus":"MANY_SEATS_AVAILABLE"}
                """;

        VehiclePosition parsed =
                Json.fromBytes(fromTheFuture.getBytes(java.nio.charset.StandardCharsets.UTF_8),
                        VehiclePosition.class);

        assertThat(parsed.vehicleId()).isEqualTo("bus-1");
        assertThat(parsed.routeId()).isEqualTo("15");
    }
}
