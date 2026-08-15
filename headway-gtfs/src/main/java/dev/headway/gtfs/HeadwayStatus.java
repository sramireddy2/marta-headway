package dev.headway.gtfs;

/**
 * How one gap between two consecutive buses compares to the timetable.
 *
 * <p>Classification is on the <b>ratio</b> of observed to scheduled headway, never on the raw
 * distance. 400 m between two buses is severe bunching on a route running every 4 minutes and
 * completely unremarkable on one running every 45. The absolute number is not interpretable; the
 * ratio is.
 *
 * <p>The thresholds follow the convention used in transit operations research, where a headway
 * under half the scheduled value is the usual working definition of bunching. They are deliberately
 * asymmetric: bunching and gapping are the same event seen from two ends — buses that clump leave
 * a hole behind them — but a hole has to grow proportionally larger before it is as noteworthy as
 * a clump is.
 */
public enum HeadwayStatus {

    /** Under a quarter of the scheduled gap. Buses are effectively travelling together. */
    SEVERE_BUNCHING,

    /** Under half. The classic definition of bunching. */
    BUNCHING,

    /** Within the normal band. Nothing to act on. */
    ON_SCHEDULE,

    /** Over 1.5x. A hole is opening up. */
    GAPPING,

    /** Over 2.5x. Riders at the next stops face a long wait. */
    SEVERE_GAPPING,

    /**
     * Buses are sitting still at a terminal.
     *
     * <p>Not a defect — an operational fact. Live data showed vehicles 3503 and 3694 parked at
     * 0.0 m and 38.1 m on route 89 for over 90 seconds, which the maths correctly reports as a
     * 38 m gap. Alerting a dispatcher about buses on layover at a depot would make the alert feed
     * worthless within a day, so they get their own category and are excluded from alerts.
     */
    LAYOVER;

    public static final double SEVERE_BUNCHING_RATIO = 0.25;
    public static final double BUNCHING_RATIO = 0.50;
    public static final double GAPPING_RATIO = 1.50;
    public static final double SEVERE_GAPPING_RATIO = 2.50;

    /** Classifies a ratio of observed headway to scheduled headway. */
    public static HeadwayStatus fromRatio(double ratio) {
        if (ratio <= SEVERE_BUNCHING_RATIO) {
            return SEVERE_BUNCHING;
        }
        if (ratio <= BUNCHING_RATIO) {
            return BUNCHING;
        }
        if (ratio >= SEVERE_GAPPING_RATIO) {
            return SEVERE_GAPPING;
        }
        if (ratio >= GAPPING_RATIO) {
            return GAPPING;
        }
        return ON_SCHEDULE;
    }

    /** Whether a dispatcher should be shown this. */
    public boolean isAlertable() {
        return this == SEVERE_BUNCHING || this == BUNCHING
                || this == GAPPING || this == SEVERE_GAPPING;
    }

    public boolean isBunching() {
        return this == SEVERE_BUNCHING || this == BUNCHING;
    }

    public boolean isGapping() {
        return this == GAPPING || this == SEVERE_GAPPING;
    }
}
