package dev.headway.gtfs;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class HeadwayStatusTest {

    @ParameterizedTest(name = "ratio {0} -> {1}")
    @CsvSource({
            "0.00, SEVERE_BUNCHING",
            "0.10, SEVERE_BUNCHING",
            "0.25, SEVERE_BUNCHING",   // boundary is inclusive
            "0.26, BUNCHING",
            "0.50, BUNCHING",          // boundary is inclusive
            "0.51, ON_SCHEDULE",
            "1.00, ON_SCHEDULE",
            "1.49, ON_SCHEDULE",
            "1.50, GAPPING",
            "2.49, GAPPING",
            "2.50, SEVERE_GAPPING",
            "9.00, SEVERE_GAPPING",
    })
    @DisplayName("ratio maps to the expected status, boundaries included")
    void classifies(double ratio, HeadwayStatus expected) {
        assertThat(HeadwayStatus.fromRatio(ratio)).isEqualTo(expected);
    }

    /**
     * The reason classification is on a ratio and never on raw metres.
     *
     * <p>The same 400 m gap is a serious problem on a frequent route and completely normal on an
     * infrequent one. Any threshold expressed in metres would be wrong for most of the network.
     */
    @Test
    @DisplayName("the same distance means different things on different routes")
    void theSameGapMeansDifferentThings() {
        double gapMetres = 400;

        // Every 4 minutes at 8 m/s: buses should be ~1920 m apart.
        double frequentExpected = 240 * 8.0;
        // Every 45 minutes at 8 m/s: ~21,600 m apart.
        double infrequentExpected = 2700 * 8.0;

        assertThat(HeadwayStatus.fromRatio(gapMetres / frequentExpected))
                .isEqualTo(HeadwayStatus.SEVERE_BUNCHING);
        assertThat(HeadwayStatus.fromRatio(gapMetres / infrequentExpected))
                .isEqualTo(HeadwayStatus.SEVERE_BUNCHING);

        // And a 20 km gap is fine on the infrequent route but a huge hole on the frequent one.
        assertThat(HeadwayStatus.fromRatio(20_000 / infrequentExpected))
                .isEqualTo(HeadwayStatus.ON_SCHEDULE);
        assertThat(HeadwayStatus.fromRatio(20_000 / frequentExpected))
                .isEqualTo(HeadwayStatus.SEVERE_GAPPING);
    }

    @Test
    @DisplayName("only the four abnormal statuses reach a dispatcher")
    void alertability() {
        assertThat(HeadwayStatus.SEVERE_BUNCHING.isAlertable()).isTrue();
        assertThat(HeadwayStatus.BUNCHING.isAlertable()).isTrue();
        assertThat(HeadwayStatus.GAPPING.isAlertable()).isTrue();
        assertThat(HeadwayStatus.SEVERE_GAPPING.isAlertable()).isTrue();

        assertThat(HeadwayStatus.ON_SCHEDULE.isAlertable()).isFalse();
        assertThat(HeadwayStatus.LAYOVER.isAlertable())
                .as("buses parked at a depot must never page anyone")
                .isFalse();
    }

    @Test
    @DisplayName("bunching and gapping are distinguishable")
    void categories() {
        assertThat(HeadwayStatus.SEVERE_BUNCHING.isBunching()).isTrue();
        assertThat(HeadwayStatus.BUNCHING.isBunching()).isTrue();
        assertThat(HeadwayStatus.GAPPING.isBunching()).isFalse();

        assertThat(HeadwayStatus.GAPPING.isGapping()).isTrue();
        assertThat(HeadwayStatus.SEVERE_GAPPING.isGapping()).isTrue();
        assertThat(HeadwayStatus.ON_SCHEDULE.isGapping()).isFalse();
    }
}
