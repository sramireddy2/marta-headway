package dev.headway.gtfs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIOException;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class GtfsStaticLoaderTest {

    private static final Instant PUBLISHED = Instant.parse("2026-06-24T16:40:00Z");

    @TempDir
    Path temp;

    private GtfsStaticLoader loader;

    @BeforeEach
    void setUp() {
        loader = new GtfsStaticLoader();
    }

    private GtfsSnapshot load(GtfsZipBuilder builder) throws IOException {
        return loader.load(builder.writeTo(temp), PUBLISHED);
    }

    @Test
    @DisplayName("routes, trips and shapes all load")
    void loadsEverything() throws IOException {
        GtfsSnapshot snapshot = load(GtfsZipBuilder.martaLike());

        assertThat(snapshot.routeCount()).isEqualTo(3);
        assertThat(snapshot.shapeCount()).isEqualTo(2);
        assertThat(snapshot.feedLastModified()).isEqualTo(PUBLISHED);
    }

    /**
     * The join that would silently return nothing if done the obvious way.
     *
     * <p>A realtime vehicle reports {@code route_id: "15"}. That is the static feed's
     * {@code route_short_name}; its {@code route_id} is 26913.
     */
    @Test
    @DisplayName("realtime route ids resolve via route_short_name, not route_id")
    void resolvesRouteByShortName() throws IOException {
        GtfsSnapshot snapshot = load(GtfsZipBuilder.martaLike());

        assertThat(snapshot.routeForRealtimeId("15"))
                .isPresent()
                .get()
                .satisfies(route -> {
                    assertThat(route.routeId()).isEqualTo("26913");
                    assertThat(route.shortName()).isEqualTo("15");
                });

        // The mistake, demonstrated: the static id is not what realtime sends.
        assertThat(snapshot.routeForRealtimeId("26913")).isEmpty();
        assertThat(snapshot.routeByStaticId("26913")).isPresent();
    }

    /**
     * {@code "Clifton Road, Candler Road"} is one quoted field containing a comma. With
     * {@code split(",")} every column after it shifts by one and route_type becomes garbage.
     */
    @Test
    @DisplayName("a comma inside a quoted field does not shift the columns")
    void handlesQuotedCommas() throws IOException {
        GtfsSnapshot snapshot = load(GtfsZipBuilder.martaLike());

        GtfsRoute route = snapshot.routeForRealtimeId("15").orElseThrow();
        assertThat(route.longName()).isEqualTo("Clifton Road, Candler Road");
        assertThat(route.routeType()).isEqualTo(3);
        assertThat(route.isBus()).isTrue();
        assertThat(route.colorHex()).isEqualTo("367AA8");
    }

    @Test
    @DisplayName("rail routes are distinguishable from buses")
    void readsRouteType() throws IOException {
        GtfsSnapshot snapshot = load(GtfsZipBuilder.martaLike());

        assertThat(snapshot.routeForRealtimeId("A").orElseThrow().isBus()).isFalse();
    }

    /** Solves the step 3 problem: the static feed's direction_id really is 0 or 1. */
    @Test
    @DisplayName("trip direction comes from the static feed and is 0 or 1")
    void readsTripDirection() throws IOException {
        GtfsSnapshot snapshot = load(GtfsZipBuilder.martaLike());

        assertThat(snapshot.trip("10001").orElseThrow().directionId()).isZero();
        assertThat(snapshot.trip("10001").orElseThrow().isOutbound()).isTrue();
        assertThat(snapshot.trip("10002").orElseThrow().directionId()).isEqualTo(1);
    }

    @Test
    @DisplayName("unusable trips are dropped at load rather than checked everywhere downstream")
    void skipsUnusableTrips() throws IOException {
        GtfsSnapshot snapshot = load(GtfsZipBuilder.martaLike());

        assertThat(snapshot.trip("10004")).as("no shape_id").isEmpty();
        assertThat(snapshot.trip("10005")).as("direction_id of 7 is not valid GTFS").isEmpty();
        assertThat(snapshot.tripCount()).isEqualTo(3);
    }

    /**
     * MARTA publishes {@code shape_dist_traveled} in kilometres; everything downstream works in
     * metres. Getting this wrong by 1000x would not throw, it would just make every headway absurd.
     */
    @Test
    @DisplayName("shape distances are converted from kilometres to metres")
    void convertsShapeDistanceToMetres() throws IOException {
        GtfsSnapshot snapshot = load(GtfsZipBuilder.martaLike());

        RouteShape shape = snapshot.shape("SHAPE_A").orElseThrow();
        assertThat(shape.pointCount()).isEqualTo(4);
        assertThat(shape.distanceAt(0)).isZero();
        assertThat(shape.distanceAt(1)).isCloseTo(111.3, org.assertj.core.data.Offset.offset(0.5));
        assertThat(shape.lengthMetres()).isCloseTo(334.0, org.assertj.core.data.Offset.offset(1.0));
    }

    /**
     * {@code shape_dist_traveled} is optional in GTFS. SHAPE_B leaves it blank, so the loader has
     * to compute cumulative distance itself — which keeps this module usable against feeds from
     * agencies other than MARTA.
     */
    @Test
    @DisplayName("missing shape_dist_traveled is computed by haversine instead")
    void computesDistanceWhenMissing() throws IOException {
        GtfsSnapshot snapshot = load(GtfsZipBuilder.martaLike());

        RouteShape shape = snapshot.shape("SHAPE_B").orElseThrow();
        assertThat(shape.distanceAt(0)).isZero();
        // Two hops of 0.001 degrees of latitude, about 111.2 m each.
        assertThat(shape.lengthMetres())
                .isCloseTo(222.4, org.assertj.core.data.Offset.offset(1.0));
        assertThat(shape.distanceAt(1)).isLessThan(shape.distanceAt(2));
    }

    @Test
    @DisplayName("a UTF-8 BOM does not corrupt the first column name")
    void stripsByteOrderMark() throws IOException {
        GtfsSnapshot snapshot = load(GtfsZipBuilder.martaLike().withByteOrderMark());

        assertThat(snapshot.routeForRealtimeId("1")).isPresent();
        assertThat(snapshot.trip("10001")).isPresent();
    }

    @Test
    @DisplayName("resolve() joins a realtime sighting to route, direction and path in one call")
    void resolvesFullContext() throws IOException {
        GtfsSnapshot snapshot = load(GtfsZipBuilder.martaLike());

        TripContext context = snapshot.resolve("15", "10003").orElseThrow();

        assertThat(context.route().shortName()).isEqualTo("15");
        assertThat(context.trip().directionId()).isZero();
        assertThat(context.shape().shapeId()).isEqualTo("SHAPE_A");
        assertThat(context.headwayGroup()).isEqualTo("15:0");
    }

    /**
     * Two buses on the same route going opposite ways must not share a headway group, or the gap
     * between them gets measured as if they were following each other.
     */
    @Test
    @DisplayName("opposite directions on one route are different headway groups")
    void directionSeparatesHeadwayGroups() throws IOException {
        GtfsSnapshot snapshot = load(GtfsZipBuilder.martaLike());

        String outbound = snapshot.resolve("1", "10001").orElseThrow().headwayGroup();
        String inbound = snapshot.resolve("1", "10002").orElseThrow().headwayGroup();

        assertThat(outbound).isEqualTo("1:0");
        assertThat(inbound).isEqualTo("1:1");
        assertThat(outbound).isNotEqualTo(inbound);
    }

    @Test
    @DisplayName("an unknown trip resolves to empty rather than a half-populated context")
    void unknownTripResolvesEmpty() throws IOException {
        GtfsSnapshot snapshot = load(GtfsZipBuilder.martaLike());

        assertThat(snapshot.resolve("15", "does-not-exist")).isEmpty();
        assertThat(snapshot.resolve("15", null)).isEmpty();
        assertThat(snapshot.shapeForTrip("10004")).isEmpty();
    }

    @Test
    @DisplayName("a missing file in the archive fails loudly")
    void missingFileFailsClearly() {
        GtfsZipBuilder incomplete = new GtfsZipBuilder()
                .file("routes.txt", "route_id,route_short_name,route_long_name,route_type\n1,1,X,3\n");

        assertThatIOException()
                .isThrownBy(() -> loader.load(incomplete.writeTo(temp), PUBLISHED))
                .withMessageContaining("trips.txt");
    }

    @Test
    @DisplayName("shape point arrays are copies, so callers cannot corrupt shared state")
    void shapeArraysAreDefensivelyCopied() throws IOException {
        GtfsSnapshot snapshot = load(GtfsZipBuilder.martaLike());
        RouteShape shape = snapshot.shape("SHAPE_A").orElseThrow();

        double[] lats = shape.latitudes();
        lats[0] = 0;

        assertThat(shape.latitudeAt(0)).isEqualTo(33.75);
    }
}
