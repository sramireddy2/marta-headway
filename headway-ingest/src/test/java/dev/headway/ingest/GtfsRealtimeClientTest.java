package dev.headway.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.transit.realtime.GtfsRealtime.FeedMessage;
import dev.headway.common.VehiclePosition;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Tests decoding against a real MARTA feed snapshot committed to {@code src/test/resources}.
 *
 * <p>Recording a fixture instead of hitting the network is the standard move. The test is fast,
 * works on a plane, works in CI, and — crucially — produces the same answer every time. A test
 * that talks to a live feed can fail because a bus went out of service, which tells you nothing
 * about your code.
 */
class GtfsRealtimeClientTest {

    private static final String FIXTURE = "/marta-vehiclepositions-sample.pb";

    private static FeedMessage feed;

    @BeforeAll
    static void loadFixture() throws IOException {
        try (InputStream in = GtfsRealtimeClientTest.class.getResourceAsStream(FIXTURE)) {
            assertThat(in).as("fixture %s must be on the test classpath", FIXTURE).isNotNull();
            feed = FeedMessage.parseFrom(in);
        }
    }

    @Test
    void decodesFeedHeader() {
        assertThat(feed.getHeader().getGtfsRealtimeVersion()).isEqualTo("2.0");
        assertThat(feed.getEntityCount()).isPositive();
    }

    @Test
    void convertsEntitiesToVehiclePositions() {
        List<VehiclePosition> positions = GtfsRealtimeClient.toVehiclePositions(feed);

        assertThat(positions).isNotEmpty();
        assertThat(positions).allSatisfy(vp -> {
            assertThat(vp.vehicleId()).isNotBlank();
            assertThat(vp.routeId()).isNotBlank();
            // Atlanta's bounding box. Catches unit mix-ups and swapped lat/lon.
            assertThat(vp.latitude()).isBetween(33.0, 34.5);
            assertThat(vp.longitude()).isBetween(-85.0, -83.5);
        });
    }

    @Test
    void everyVehicleAppearsAtMostOncePerFeed() {
        List<VehiclePosition> positions = GtfsRealtimeClient.toVehiclePositions(feed);

        assertThat(positions).extracting(VehiclePosition::vehicleId).doesNotHaveDuplicates();
    }

    @Test
    void supersessionComparesTimestampsForTheSameVehicle() {
        List<VehiclePosition> positions = GtfsRealtimeClient.toVehiclePositions(feed);
        VehiclePosition older = positions.getFirst();
        VehiclePosition newer = new VehiclePosition(
                older.vehicleId(), older.routeId(), older.tripId(), older.directionId(),
                older.latitude(), older.longitude(), older.bearingDegrees(),
                older.speedMetersPerSecond(), older.timestamp().plusSeconds(30));

        assertThat(older.isSupersededBy(newer)).isTrue();
        assertThat(newer.isSupersededBy(older)).isFalse();
        assertThat(older.isSupersededBy(older)).as("a re-delivery is not newer").isFalse();
    }
}
