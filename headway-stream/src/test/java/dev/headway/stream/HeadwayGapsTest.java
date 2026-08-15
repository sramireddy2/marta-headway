package dev.headway.stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import dev.headway.stream.HeadwayGaps.Sighting;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Tests for the headway calculation, with no Spark involved.
 *
 * <p>The numbers here are chosen so the right answer is obvious by inspection. A gap calculation
 * that is subtly wrong still returns plausible-looking metres, so asserting against whatever the
 * code emits would prove nothing.
 */
class HeadwayGapsTest {

    private static final long T0 = 1_000_000L;

    private static Sighting at(String vehicleId, double metres) {
        return new Sighting(vehicleId, metres, T0);
    }

    private static Sighting at(String vehicleId, double metres, long millis) {
        return new Sighting(vehicleId, metres, millis);
    }

    @Nested
    @DisplayName("the basic difference")
    class BasicDifference {

        @Test
        @DisplayName("two buses produce one gap equal to their separation")
        void twoBuses() {
            HeadwayGaps.Result r = HeadwayGaps.compute(List.of(at("a", 1000), at("b", 2500)));

            assertThat(r.vehicleCount()).isEqualTo(2);
            assertThat(r.gapsMetres()).containsExactly(1500.0);
            assertThat(r.minGapMetres()).isEqualTo(1500.0);
            assertThat(r.maxGapMetres()).isEqualTo(1500.0);
            assertThat(r.medianGapMetres()).isEqualTo(1500.0);
            assertThat(r.meanGapMetres()).isEqualTo(1500.0);
        }

        @Test
        @DisplayName("four evenly spaced buses produce three equal gaps")
        void evenlySpaced() {
            HeadwayGaps.Result r = HeadwayGaps.compute(
                    List.of(at("a", 0), at("b", 1000), at("c", 2000), at("d", 3000)));

            assertThat(r.gapsMetres()).containsExactly(1000.0, 1000.0, 1000.0);
            assertThat(r.orderedVehicles()).containsExactly("a", "b", "c", "d");
        }

        @Test
        @DisplayName("one bunched pair and one big hole - the signature of bunching")
        void bunchedAndGapped() {
            HeadwayGaps.Result r = HeadwayGaps.compute(
                    List.of(at("a", 5000), at("b", 5020), at("c", 14000)));

            assertThat(r.gapsMetres()).containsExactly(20.0, 8980.0);
            assertThat(r.minGapMetres()).isEqualTo(20.0);
            assertThat(r.maxGapMetres()).isEqualTo(8980.0);
        }

        @Test
        @DisplayName("gaps are never negative, whatever order the input arrives in")
        void gapsAreNeverNegative() {
            HeadwayGaps.Result r = HeadwayGaps.compute(
                    List.of(at("c", 9000), at("a", 100), at("d", 12000), at("b", 4000)));

            assertThat(r.gapsMetres()).allSatisfy(g -> assertThat(g).isNotNegative());
        }
    }

    @Nested
    @DisplayName("deduplicating to the newest reading per vehicle")
    class Deduplication {

        /**
         * The bug this whole class exists to prevent.
         *
         * <p>A 60-second window at 15-second polling holds about four sightings of the same bus.
         * Without deduplication those become "gaps" of a few metres between a bus and itself
         * moments earlier, and the system reports severe bunching on every route, permanently.
         */
        @Test
        @DisplayName("one bus seen four times is one bus, not four buses metres apart")
        void oneBusIsNotAConvoy() {
            HeadwayGaps.Result r = HeadwayGaps.compute(List.of(
                    at("bus-1", 1000, T0),
                    at("bus-1", 1080, T0 + 15_000),
                    at("bus-1", 1165, T0 + 30_000),
                    at("bus-1", 1240, T0 + 45_000)));

            assertThat(r.vehicleCount()).as("one vehicle").isEqualTo(1);
            assertThat(r.gapsMetres()).as("a bus has no gap with itself").isEmpty();
            assertThat(r.minGapMetres()).isNull();
            assertThat(r.orderedDistancesMetres()).containsExactly(1240.0);
        }

        @Test
        @DisplayName("the newest reading wins, not the last one in the list")
        void newestWinsRegardlessOfInputOrder() {
            HeadwayGaps.Result r = HeadwayGaps.compute(List.of(
                    at("bus-1", 5000, T0 + 30_000),   // newest, but first in the list
                    at("bus-1", 1000, T0),
                    at("bus-1", 3000, T0 + 15_000)));

            assertThat(r.orderedDistancesMetres()).containsExactly(5000.0);
        }

        @Test
        @DisplayName("two buses seen repeatedly still produce exactly one gap")
        void repeatedSightingsOfTwoBuses() {
            HeadwayGaps.Result r = HeadwayGaps.compute(List.of(
                    at("a", 1000, T0), at("b", 4000, T0),
                    at("a", 1100, T0 + 15_000), at("b", 4200, T0 + 15_000),
                    at("a", 1250, T0 + 30_000), at("b", 4350, T0 + 30_000)));

            assertThat(r.vehicleCount()).isEqualTo(2);
            assertThat(r.gapsMetres()).containsExactly(3100.0); // 4350 - 1250, the newest of each
        }

        @Test
        @DisplayName("identical timestamps are resolved deterministically, not randomly")
        void tiedTimestampsAreStable() {
            List<Sighting> input = List.of(at("a", 1000, T0), at("a", 1000, T0));

            // Replay must give the same answer or a restart would produce different output.
            for (int i = 0; i < 20; i++) {
                assertThat(HeadwayGaps.compute(input).orderedDistancesMetres())
                        .containsExactly(1000.0);
            }
        }
    }

    @Nested
    @DisplayName("ordering along the route")
    class Ordering {

        /**
         * Shuffled input must always give the same answer. Kafka delivers by partition order, not
         * by position on the road, so arrival order carries no geographic meaning at all.
         */
        @Test
        @DisplayName("input order does not affect the result")
        void inputOrderIsIrrelevant() {
            List<Sighting> input = new ArrayList<>(List.of(
                    at("a", 0), at("b", 1500), at("c", 4000), at("d", 4100), at("e", 9000)));

            List<Double> expected = List.of(1500.0, 2500.0, 100.0, 4900.0);

            for (int i = 0; i < 25; i++) {
                Collections.shuffle(input);
                HeadwayGaps.Result r = HeadwayGaps.compute(input);

                assertThat(r.gapsMetres()).isEqualTo(expected);
                assertThat(r.orderedVehicles()).containsExactly("a", "b", "c", "d", "e");
            }
        }

        @Test
        @DisplayName("distances come out sorted")
        void distancesAreSorted() {
            HeadwayGaps.Result r = HeadwayGaps.compute(
                    List.of(at("c", 300), at("a", 100), at("b", 200)));

            assertThat(r.orderedDistancesMetres()).isSorted();
            assertThat(r.orderedVehicles()).containsExactly("a", "b", "c");
        }

        /**
         * Two buses at the same distance happens at terminals, where several sit on layover. The
         * tie-break on vehicle id keeps the output replayable.
         */
        @Test
        @DisplayName("buses at an identical distance order stably by id")
        void tiedDistancesBreakOnVehicleId() {
            for (int i = 0; i < 10; i++) {
                HeadwayGaps.Result r = HeadwayGaps.compute(
                        List.of(at("zebra", 500), at("alpha", 500)));

                assertThat(r.orderedVehicles()).containsExactly("alpha", "zebra");
                assertThat(r.gapsMetres()).containsExactly(0.0);
            }
        }
    }

    @Nested
    @DisplayName("statistics")
    class Statistics {

        @Test
        @DisplayName("median of an odd number of gaps is the middle one")
        void medianOdd() {
            HeadwayGaps.Result r = HeadwayGaps.compute(
                    List.of(at("a", 0), at("b", 100), at("c", 1100), at("d", 6100)));

            assertThat(r.gapsMetres()).containsExactly(100.0, 1000.0, 5000.0);
            assertThat(r.medianGapMetres()).isEqualTo(1000.0);
        }

        @Test
        @DisplayName("median of an even number of gaps averages the middle two")
        void medianEven() {
            HeadwayGaps.Result r = HeadwayGaps.compute(
                    List.of(at("a", 0), at("b", 100), at("c", 1100), at("d", 3100), at("e", 9100)));

            assertThat(r.gapsMetres()).containsExactly(100.0, 1000.0, 2000.0, 6000.0);
            assertThat(r.medianGapMetres()).isEqualTo(1500.0);
        }

        /**
         * Median rather than mean is the point. One enormous hole drags the mean far above what
         * most buses on the route are experiencing.
         */
        @Test
        @DisplayName("the median resists an outlier that the mean does not")
        void medianResistsOutliers() {
            HeadwayGaps.Result r = HeadwayGaps.compute(
                    List.of(at("a", 0), at("b", 1000), at("c", 2000), at("d", 40000)));

            assertThat(r.medianGapMetres()).isEqualTo(1000.0);
            assertThat(r.meanGapMetres()).isCloseTo(13333.3, within(0.1));
        }

        @Test
        @DisplayName("mean is the total spread divided by the number of gaps")
        void mean() {
            HeadwayGaps.Result r = HeadwayGaps.compute(
                    List.of(at("a", 0), at("b", 1000), at("c", 3000)));

            assertThat(r.meanGapMetres()).isCloseTo(1500.0, within(1e-9));
        }
    }

    /**
     * The layover filter, driven by what step 7 actually produced against live data: vehicles 3503
     * and 3694 sat at 0.0 m and 38.1 m on route 89 for over 90 seconds and were reported as a
     * 38 m gap every window. Correct arithmetic, useless alert.
     */
    @Nested
    @DisplayName("excluding buses parked at a terminal")
    class LayoverFiltering {

        private static final double ROUTE_LENGTH = 21_600;

        @Test
        @DisplayName("the real route 89 case: two parked buses stop producing a phantom gap")
        void theRealCase() {
            List<Sighting> window = List.of(
                    // Two buses at the terminal, not moving across three sightings each.
                    at("3503", 0.0, T0), at("3503", 0.0, T0 + 15_000), at("3503", 0.0, T0 + 30_000),
                    at("3694", 38.1, T0), at("3694", 38.1, T0 + 15_000), at("3694", 38.1, T0 + 30_000),
                    // Two genuinely running.
                    at("3519", 10_100, T0), at("3519", 10_400, T0 + 30_000),
                    at("3507", 13_000, T0), at("3507", 13_300, T0 + 30_000));

            HeadwayGaps.Result r = HeadwayGaps.compute(window, ROUTE_LENGTH);

            assertThat(r.layoverVehicles()).containsExactly("3503", "3694");
            assertThat(r.vehicleCount()).isEqualTo(2);
            assertThat(r.gapsMetres()).containsExactly(2900.0);
            assertThat(r.minGapMetres())
                    .as("the 38 m phantom gap is gone")
                    .isEqualTo(2900.0);
        }

        /**
         * Both conditions are required. A bus stopped at a red light or dwelling at a busy stop is
         * stationary too — and those are exactly the conditions that cause bunching, so excluding
         * them would blind the detector to the thing it exists to find.
         */
        @Test
        @DisplayName("a stationary bus mid-route is kept, because that is what bunching looks like")
        void stationaryMidRouteIsKept() {
            List<Sighting> window = List.of(
                    at("stuck", 9000, T0), at("stuck", 9005, T0 + 30_000),
                    at("behind", 9100, T0), at("behind", 9200, T0 + 30_000));

            HeadwayGaps.Result r = HeadwayGaps.compute(window, ROUTE_LENGTH);

            assertThat(r.layoverVehicles()).isEmpty();
            assertThat(r.vehicleCount()).isEqualTo(2);
        }

        @Test
        @DisplayName("a moving bus near a terminal is kept — it is starting its run")
        void movingNearTerminalIsKept() {
            List<Sighting> window = List.of(
                    at("departing", 20, T0), at("departing", 300, T0 + 30_000),
                    at("ahead", 2000, T0), at("ahead", 2300, T0 + 30_000));

            HeadwayGaps.Result r = HeadwayGaps.compute(window, ROUTE_LENGTH);

            assertThat(r.layoverVehicles()).isEmpty();
            assertThat(r.vehicleCount()).isEqualTo(2);
        }

        @Test
        @DisplayName("the far terminal counts too, not just the start")
        void endOfRouteAlsoCounts() {
            List<Sighting> window = List.of(
                    at("arrived", ROUTE_LENGTH - 10, T0), at("arrived", ROUTE_LENGTH - 10, T0 + 30_000),
                    at("running", 5000, T0), at("running", 5300, T0 + 30_000));

            HeadwayGaps.Result r = HeadwayGaps.compute(window, ROUTE_LENGTH);

            assertThat(r.layoverVehicles()).containsExactly("arrived");
        }

        @Test
        @DisplayName("GPS jitter on a parked bus does not make it look like it moved")
        void jitterIsNotMovement() {
            List<Sighting> window = List.of(
                    at("parked", 10, T0), at("parked", 22, T0 + 15_000), at("parked", 4, T0 + 30_000),
                    at("running", 5000, T0), at("running", 5300, T0 + 30_000));

            HeadwayGaps.Result r = HeadwayGaps.compute(window, ROUTE_LENGTH);

            assertThat(r.layoverVehicles()).containsExactly("parked");
        }

        @Test
        @DisplayName("without a route length the filter is skipped entirely")
        void noRouteLengthSkipsTheFilter() {
            List<Sighting> window = List.of(at("3503", 0.0, T0), at("3694", 38.1, T0));

            HeadwayGaps.Result r = HeadwayGaps.compute(window);

            assertThat(r.layoverVehicles()).isEmpty();
            assertThat(r.gapsMetres()).containsExactly(38.1);
        }

        /** Movement is measured before deduplication, or there would be nothing to measure. */
        @Test
        @DisplayName("movement is judged across all sightings, not just the newest")
        void movementUsesTheWholeWindow() {
            List<Sighting> window = List.of(
                    at("mover", 5, T0), at("mover", 400, T0 + 30_000),
                    at("other", 3000, T0), at("other", 3300, T0 + 30_000));

            HeadwayGaps.Result r = HeadwayGaps.compute(window, ROUTE_LENGTH);

            assertThat(r.layoverVehicles())
                    .as("it moved 395 m, so it is running despite ending near the start")
                    .isEmpty();
        }
    }

    @Nested
    @DisplayName("edge cases")
    class EdgeCases {

        @Test
        @DisplayName("no sightings")
        void empty() {
            HeadwayGaps.Result r = HeadwayGaps.compute(List.of());

            assertThat(r.vehicleCount()).isZero();
            assertThat(r.gapsMetres()).isEmpty();
            assertThat(r.minGapMetres()).isNull();
            assertThat(r.medianGapMetres()).isNull();
        }

        @Test
        @DisplayName("null input is treated as empty rather than throwing")
        void nullInput() {
            assertThat(HeadwayGaps.compute(null).vehicleCount()).isZero();
        }

        @Test
        @DisplayName("a lone bus has no gap")
        void singleVehicle() {
            HeadwayGaps.Result r = HeadwayGaps.compute(List.of(at("only", 4321)));

            assertThat(r.vehicleCount()).isEqualTo(1);
            assertThat(r.gapsMetres()).isEmpty();
            assertThat(r.orderedDistancesMetres()).containsExactly(4321.0);
        }

        /**
         * Reproduces what live data actually produced: two buses parked at a terminal, at 0.0 m
         * and 38.1 m, unchanged across windows. The maths here is right — they really are 38 m
         * apart. Deciding that this is layover rather than bunching is step 8's job, and this test
         * pins the input so that change can be made against a known case.
         */
        @Test
        @DisplayName("buses parked at a terminal are reported as a real, tiny gap")
        void terminalLayoverIsStillATinyGap() {
            HeadwayGaps.Result r = HeadwayGaps.compute(List.of(
                    at("3503", 0.0), at("3694", 38.1), at("3519", 10133.4), at("3507", 13165.8)));

            assertThat(r.gapsMetres().get(0)).isCloseTo(38.1, within(0.01));
            assertThat(r.minGapMetres()).isCloseTo(38.1, within(0.01));
            assertThat(r.orderedVehicles()).containsExactly("3503", "3694", "3519", "3507");
        }

        @Test
        @DisplayName("returned collections are immutable, so callers cannot corrupt a result")
        void resultsAreImmutable() {
            HeadwayGaps.Result r = HeadwayGaps.compute(List.of(at("a", 0), at("b", 100)));

            assertThat(r.gapsMetres()).isUnmodifiable();
            assertThat(r.orderedVehicles()).isUnmodifiable();
            assertThat(r.orderedDistancesMetres()).isUnmodifiable();
        }
    }
}
