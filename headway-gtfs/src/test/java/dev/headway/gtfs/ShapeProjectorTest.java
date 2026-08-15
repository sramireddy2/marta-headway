package dev.headway.gtfs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Geometry that is subtly wrong still returns plausible-looking numbers, so these tests check
 * against distances worked out by hand rather than against whatever the code happens to produce.
 *
 * <p>Most fixtures are built from due-north or due-east lines, where the expected answer is
 * arithmetic: 0.001 degrees of latitude is 111.195 m everywhere, and 0.001 degrees of longitude at
 * latitude 33.75 is {@code 111.195 * cos(33.75) = 92.45 m}.
 */
class ShapeProjectorTest {

    private static final double BASE_LAT = 33.750000;
    private static final double BASE_LON = -84.390000;

    /** Metres in 0.001 degrees of latitude. */
    private static final double M_PER_MILLIDEG_LAT = 111.195;

    /** A straight line running due north from the base point, {@code points} vertices apart. */
    private static RouteShape northLine(int points, double milliDegreeStep) {
        double[] lat = new double[points];
        double[] lon = new double[points];
        double[] dist = new double[points];
        for (int i = 0; i < points; i++) {
            lat[i] = BASE_LAT + i * milliDegreeStep / 1000.0;
            lon[i] = BASE_LON;
            dist[i] = i * milliDegreeStep * M_PER_MILLIDEG_LAT;
        }
        return new RouteShape("NORTH", lat, lon, dist);
    }

    private static RouteShape shapeOf(double[][] latLonDist) {
        int n = latLonDist.length;
        double[] lat = new double[n];
        double[] lon = new double[n];
        double[] dist = new double[n];
        for (int i = 0; i < n; i++) {
            lat[i] = latLonDist[i][0];
            lon[i] = latLonDist[i][1];
            dist[i] = latLonDist[i][2];
        }
        return new RouteShape("TEST", lat, lon, dist);
    }

    @Nested
    @DisplayName("points that lie exactly on the path")
    class OnThePath {

        @Test
        @DisplayName("a vertex projects to its own cumulative distance with no cross-track error")
        void projectsOntoVertices() {
            RouteShape shape = northLine(5, 1.0);

            for (int i = 0; i < shape.pointCount(); i++) {
                ShapeProjection p =
                        ShapeProjector.project(shape, shape.latitudeAt(i), shape.longitudeAt(i));

                assertThat(p.crossTrackMetres()).as("vertex %d", i).isCloseTo(0, within(0.01));
                assertThat(p.distanceAlongRouteMetres())
                        .as("vertex %d", i).isCloseTo(shape.distanceAt(i), within(0.01));
            }
        }

        @Test
        @DisplayName("the midpoint of a segment projects to half its length")
        void projectsOntoSegmentMidpoint() {
            RouteShape shape = northLine(3, 1.0); // 0, 111.195, 222.39 m

            ShapeProjection p = ShapeProjector.project(shape, BASE_LAT + 0.0005, BASE_LON);

            assertThat(p.crossTrackMetres()).isCloseTo(0, within(0.01));
            assertThat(p.distanceAlongRouteMetres()).isCloseTo(55.6, within(0.5));
            assertThat(p.segmentIndex()).isZero();
        }
    }

    @Nested
    @DisplayName("points beside the path")
    class BesideThePath {

        /**
         * A point due east of the midpoint. The along-route distance must be unchanged and the
         * cross-track distance must equal the eastward offset — 0.001 degrees of longitude at this
         * latitude, which is 111.195 * cos(33.75) = 92.45 m, NOT 111.195 m. Getting that wrong is
         * exactly what happens if you do planar geometry on raw degrees.
         */
        @Test
        @DisplayName("cross-track uses real metres, so longitude is scaled by cos(latitude)")
        void scalesLongitudeByCosLatitude() {
            RouteShape shape = northLine(3, 1.0);
            double expectedEastMetres = M_PER_MILLIDEG_LAT * Math.cos(Math.toRadians(BASE_LAT));

            ShapeProjection p =
                    ShapeProjector.project(shape, BASE_LAT + 0.0005, BASE_LON + 0.001);

            assertThat(expectedEastMetres).isCloseTo(92.45, within(0.1)); // the fixture's own maths
            assertThat(p.crossTrackMetres()).isCloseTo(expectedEastMetres, within(0.2));
            assertThat(p.distanceAlongRouteMetres())
                    .as("moving sideways must not change progress along the route")
                    .isCloseTo(55.6, within(0.5));
        }

        @Test
        @DisplayName("the snapped point lands back on the path")
        void reportsSnappedPoint() {
            RouteShape shape = northLine(3, 1.0);

            ShapeProjection p =
                    ShapeProjector.project(shape, BASE_LAT + 0.0005, BASE_LON + 0.001);

            assertThat(p.snappedLongitude()).isCloseTo(BASE_LON, within(1e-9));
            assertThat(p.snappedLatitude()).isCloseTo(BASE_LAT + 0.0005, within(1e-9));
        }
    }

    @Nested
    @DisplayName("points beyond the ends")
    class BeyondTheEnds {

        /**
         * Clamping {@code t} to [0,1] is what makes this a segment rather than an infinite line.
         * Without it, a point south of the start would project onto an imaginary extension of the
         * road and report a negative distance along the route.
         */
        @Test
        @DisplayName("a point before the start clamps to zero, never negative")
        void clampsBeforeStart() {
            RouteShape shape = northLine(3, 1.0);

            ShapeProjection p = ShapeProjector.project(shape, BASE_LAT - 0.005, BASE_LON);

            assertThat(p.distanceAlongRouteMetres()).isZero();
            assertThat(p.crossTrackMetres()).isCloseTo(5 * M_PER_MILLIDEG_LAT, within(1.0));
        }

        @Test
        @DisplayName("a point past the end clamps to the full length")
        void clampsAfterEnd() {
            RouteShape shape = northLine(3, 1.0);

            ShapeProjection p = ShapeProjector.project(shape, BASE_LAT + 0.005, BASE_LON);

            assertThat(p.distanceAlongRouteMetres()).isCloseTo(shape.lengthMetres(), within(0.01));
            assertThat(p.crossTrackMetres()).isCloseTo(3 * M_PER_MILLIDEG_LAT, within(1.0));
        }
    }

    @Nested
    @DisplayName("corners and bends")
    class Corners {

        /** An L-shape: 1 km north, then 1 km east. A point past the corner must round the bend. */
        @Test
        @DisplayName("a right-angle turn is followed, not cut across")
        void followsARightAngle() {
            double eastDegreesFor1Km = 1000.0 / (111_195.08 * Math.cos(Math.toRadians(33.759)));
            RouteShape shape = shapeOf(new double[][] {
                    {BASE_LAT, BASE_LON, 0},
                    {BASE_LAT + 0.009, BASE_LON, 1000},
                    {BASE_LAT + 0.009, BASE_LON + eastDegreesFor1Km, 2000},
            });

            // Halfway along the eastward leg.
            ShapeProjection p = ShapeProjector.project(
                    shape, BASE_LAT + 0.009, BASE_LON + eastDegreesFor1Km / 2);

            assertThat(p.segmentIndex()).isEqualTo(1);
            assertThat(p.distanceAlongRouteMetres()).isCloseTo(1500.0, within(5.0));
            assertThat(p.crossTrackMetres()).isCloseTo(0.0, within(1.0));
        }
    }

    @Nested
    @DisplayName("looping routes, where the naive answer is ambiguous")
    class Loops {

        /**
         * An out-and-back: north 1 km, then straight back south along the same street. Every point
         * on it is near two segments that are 1 km apart in route distance.
         *
         * <p>A global nearest-segment search has to pick one, and GPS jitter of a few metres can
         * flip it — making the bus appear to jump a kilometre backwards between polls. This is a
         * real failure mode on loop routes, not a contrived one.
         */
        private RouteShape outAndBack() {
            return shapeOf(new double[][] {
                    {BASE_LAT, BASE_LON, 0},
                    {BASE_LAT + 0.009, BASE_LON, 1000},
                    {BASE_LAT + 0.009, BASE_LON + 0.00001, 1001},
                    {BASE_LAT, BASE_LON + 0.00001, 2001},
            });
        }

        @Test
        @DisplayName("without a hint, a point on the doubled section is genuinely ambiguous")
        void unhintedIsAmbiguous() {
            RouteShape shape = outAndBack();

            ShapeProjection p = ShapeProjector.project(shape, BASE_LAT + 0.0045, BASE_LON);

            // It matches one of the two passes. Which one is not something we can rely on.
            assertThat(p.distanceAlongRouteMetres())
                    .satisfiesAnyOf(
                            d -> assertThat(d).isCloseTo(500.0, within(20.0)),
                            d -> assertThat(d).isCloseTo(1500.0, within(20.0)));
        }

        @Test
        @DisplayName("a hint picks the pass the bus is actually on")
        void hintResolvesTheAmbiguity() {
            RouteShape shape = outAndBack();
            double lat = BASE_LAT + 0.0045;

            ShapeProjection outbound = ShapeProjector.projectNear(shape, lat, BASE_LON, 480, 200);
            ShapeProjection inbound = ShapeProjector.projectNear(shape, lat, BASE_LON, 1520, 200);

            assertThat(outbound.distanceAlongRouteMetres()).isCloseTo(500.0, within(20.0));
            assertThat(inbound.distanceAlongRouteMetres()).isCloseTo(1500.0, within(20.0));
        }

        @Test
        @DisplayName("a hint far from anything falls back to a full scan rather than failing")
        void hintFallsBackWhenWindowIsEmpty() {
            RouteShape shape = outAndBack();

            ShapeProjection p =
                    ShapeProjector.projectNear(shape, BASE_LAT + 0.0045, BASE_LON, 50_000, 100);

            assertThat(p.crossTrackMetres()).isLessThan(50);
        }
    }

    @Nested
    @DisplayName("behaviour over a sequence of positions")
    class Sequences {

        /**
         * A bus walking the length of a shape must produce strictly increasing distances. A single
         * segment indexed or interpolated wrongly shows up here as a step backwards, which no
         * single-point test would catch.
         */
        @Test
        @DisplayName("a bus moving forward never appears to move backwards")
        void progressIsMonotonic() {
            RouteShape shape = northLine(50, 1.0);
            List<Double> distances = new ArrayList<>();

            for (int step = 0; step <= 200; step++) {
                double lat = BASE_LAT + (0.049 * step / 200.0);
                // A little east-west GPS scatter, as a real fix would have.
                double lon = BASE_LON + ((step % 3) - 1) * 0.00002;
                distances.add(ShapeProjector.project(shape, lat, lon).distanceAlongRouteMetres());
            }

            assertThat(distances).isSorted();
            assertThat(distances.get(0)).isCloseTo(0, within(1.0));
            assertThat(distances.get(200)).isCloseTo(shape.lengthMetres(), within(1.0));
        }

        @Test
        @DisplayName("the gap between two buses is a subtraction once both are projected")
        void twoBusesGiveAGap() {
            RouteShape shape = northLine(50, 1.0);

            double leader = ShapeProjector.project(shape, BASE_LAT + 0.030, BASE_LON)
                    .distanceAlongRouteMetres();
            double follower = ShapeProjector.project(shape, BASE_LAT + 0.020, BASE_LON)
                    .distanceAlongRouteMetres();

            // 0.010 degrees of latitude is 1111.95 m of road.
            assertThat(leader - follower).isCloseTo(1111.95, within(2.0));
        }
    }

    @Nested
    @DisplayName("the hinted scan agrees with the full scan")
    class HintedMatchesFull {

        @Test
        @DisplayName("on a non-looping route a hint changes nothing but the work done")
        void sameAnswerOnASimpleRoute() {
            RouteShape shape = northLine(200, 1.0);

            for (int step = 0; step < 100; step++) {
                double lat = BASE_LAT + 0.199 * step / 100.0;
                ShapeProjection full = ShapeProjector.project(shape, lat, BASE_LON + 0.0001);
                ShapeProjection hinted = ShapeProjector.projectNear(
                        shape, lat, BASE_LON + 0.0001, full.distanceAlongRouteMetres(), 300);

                assertThat(hinted.distanceAlongRouteMetres())
                        .isCloseTo(full.distanceAlongRouteMetres(), within(0.01));
            }
        }
    }

    @Test
    @DisplayName("isOnRoute separates ordinary GPS scatter from an off-route vehicle")
    void flagsOffRouteVehicles() {
        RouteShape shape = northLine(10, 1.0);

        ShapeProjection near = ShapeProjector.project(shape, BASE_LAT + 0.004, BASE_LON + 0.0002);
        ShapeProjection far = ShapeProjector.project(shape, BASE_LAT + 0.004, BASE_LON + 0.02);

        assertThat(near.isOnRoute(100)).isTrue();
        assertThat(far.crossTrackMetres()).isGreaterThan(1000);
        assertThat(far.isOnRoute(100)).isFalse();
    }
}
