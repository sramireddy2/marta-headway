package dev.headway.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

import dev.headway.api.model.RouteHeadway;
import dev.headway.common.Json;
import dev.headway.gtfs.HeadwayStatus;
import java.time.Instant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * The contract with the Spark job.
 *
 * <p>These are the tests that catch a rename. Nothing else in the system will: a mismatched field
 * name produces a null, a null produces a blank cell, and a blank cell looks like a quiet route.
 */
class RouteHeadwayJsonTest {

    private static final Instant WINDOW_END = Instant.parse("2026-08-15T14:31:00Z");

    @Test
    @DisplayName("a record from the stream job parses field for field")
    void parsesTheRealShape() throws Exception {
        String json = Payloads.headway("10:1", WINDOW_END, "SEVERE_BUNCHING", 0.22, 2713.0);

        RouteHeadway headway = Json.mapper().readValue(json, RouteHeadway.class);

        assertThat(headway.headwayGroup()).isEqualTo("10:1");
        assertThat(headway.routeShortName()).isEqualTo("10");
        assertThat(headway.routeLongName()).isEqualTo("AUC / Hollywood Road");
        assertThat(headway.directionId()).isEqualTo(1);
        assertThat(headway.vehicleCount()).isEqualTo(4);
        assertThat(headway.windowEnd()).isEqualTo(WINDOW_END);
        assertThat(headway.windowStart()).isEqualTo(WINDOW_END.minusSeconds(60));
        assertThat(headway.minGapMetres()).isEqualTo(2713.0);
        assertThat(headway.gapsMetres()).containsExactly(2713.0, 5000.0, 9000.0);
        assertThat(headway.orderedVehicles()).containsExactly("4663", "4671", "4680", "4690");
        assertThat(headway.worstPairVehicles()).containsExactly("4663", "4671");
        assertThat(headway.scheduledHeadwaySeconds()).isEqualTo(1800);
        assertThat(headway.expectedSpacingMetres()).isCloseTo(12503.0, within(0.01));
        assertThat(headway.statusEnum()).isEqualTo(HeadwayStatus.SEVERE_BUNCHING);
        assertThat(headway.isAlertable()).isTrue();
    }

    /**
     * Spark drops null fields entirely. Boxed types are what make the difference between "no
     * timetable for this route" and "a scheduled headway of zero seconds" visible.
     */
    @Test
    @DisplayName("a record with no schedule leaves the comparison fields null, not zero")
    void missingScheduleIsNullNotZero() throws Exception {
        String json = Payloads.headwayWithoutSchedule("999:0", WINDOW_END);

        RouteHeadway headway = Json.mapper().readValue(json, RouteHeadway.class);

        assertThat(headway.scheduledHeadwaySeconds()).isNull();
        assertThat(headway.expectedSpacingMetres()).isNull();
        assertThat(headway.headwayRatio()).isNull();
        assertThat(headway.status()).isNull();
        assertThat(headway.statusEnum()).isNull();
        assertThat(headway.isAlertable())
                .as("no schedule means no verdict, which must not read as an alert")
                .isFalse();
        // The parts that do not depend on a timetable are still there and still usable.
        assertThat(headway.minGapMetres()).isEqualTo(400.0);
        assertThat(headway.vehicleCount()).isEqualTo(2);
    }

    @Test
    @DisplayName("an unknown field from a newer producer is skipped, not fatal")
    void toleratesNewFields() throws Exception {
        String json = """
                {"headwayGroup":"1:0","windowEnd":"2026-08-15T14:31:00Z","vehicleCount":2,\
                "somethingAddedLater":{"nested":true}}""";

        RouteHeadway headway = Json.mapper().readValue(json, RouteHeadway.class);

        assertThat(headway.headwayGroup()).isEqualTo("1:0");
    }

    @Test
    @DisplayName("a status this version does not know about is null, not an exception")
    void unknownStatusDoesNotThrow() throws Exception {
        String json = """
                {"headwayGroup":"1:0","windowEnd":"2026-08-15T14:31:00Z","status":"TELEPORTING"}""";

        RouteHeadway headway = Json.mapper().readValue(json, RouteHeadway.class);

        assertThat(headway.status()).isEqualTo("TELEPORTING");
        assertThat(headway.statusEnum()).isNull();
        assertThat(headway.isAlertable()).isFalse();
    }

    @Nested
    @DisplayName("timestamps, which Spark writes in more than one shape")
    class Timestamps {

        private RouteHeadway parse(String windowEnd) throws Exception {
            return Json.mapper().readValue(
                    "{\"headwayGroup\":\"1:0\",\"windowEnd\":" + windowEnd + "}",
                    RouteHeadway.class);
        }

        @Test
        @DisplayName("ISO-8601 with a Z")
        void withOffset() throws Exception {
            assertThat(parse("\"2026-08-15T14:31:00Z\"").windowEnd())
                    .isEqualTo(Instant.parse("2026-08-15T14:31:00Z"));
            assertThat(parse("\"2026-08-15T14:31:00.500Z\"").windowEnd())
                    .isEqualTo(Instant.parse("2026-08-15T14:31:00.500Z"));
        }

        /** Jackson's stock Instant deserializer rejects this one. The stream job runs in UTC. */
        @Test
        @DisplayName("no offset at all, which is read as UTC")
        void withoutOffset() throws Exception {
            assertThat(parse("\"2026-08-15T14:31:00.000\"").windowEnd())
                    .isEqualTo(Instant.parse("2026-08-15T14:31:00Z"));
            assertThat(parse("\"2026-08-15T14:31:00\"").windowEnd())
                    .isEqualTo(Instant.parse("2026-08-15T14:31:00Z"));
        }

        @Test
        @DisplayName("epoch milliseconds")
        void asNumber() throws Exception {
            assertThat(parse("1786804260000").windowEnd())
                    .isEqualTo(Instant.parse("2026-08-15T14:31:00Z"));
        }

        @Test
        @DisplayName("genuine rubbish still fails, rather than being guessed at")
        void nonsense() {
            assertThatThrownBy(() -> parse("\"yesterday afternoon\""))
                    .hasMessageContaining("Unparseable timestamp");
        }
    }
}
