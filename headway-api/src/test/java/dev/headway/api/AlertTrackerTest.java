package dev.headway.api;

import static org.assertj.core.api.Assertions.assertThat;

import dev.headway.api.model.AlertEpisode;
import dev.headway.api.model.RouteHeadway;
import dev.headway.api.state.AlertTracker;
import dev.headway.gtfs.HeadwayStatus;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/** Turning a stream of overlapping window measurements into distinct events. */
class AlertTrackerTest {

    private static final Instant START = Instant.parse("2026-08-15T14:00:00Z");

    private final AlertTracker tracker = new AlertTracker(10);

    /** One window's worth of measurement for route 10 inbound. */
    private static RouteHeadway window(int secondsIn, String status, double ratio) {
        return new RouteHeadway(
                START.plusSeconds(secondsIn - 60), START.plusSeconds(secondsIn),
                "10:1", "10", "AUC / Hollywood Road", 1, 24000.0,
                4, 2713.0, 5000.0, 9000.0, 5571.0,
                List.of(2713.0, 5000.0, 9000.0),
                List.of("4663", "4671", "4680", "4690"),
                List.of(100.0, 2813.0, 7813.0, 16813.0),
                List.of(),
                1800, 12503.0, 6.95, 390.5, ratio, status,
                List.of("4663", "4671"));
    }

    @Nested
    @DisplayName("the duplicate-alert fix")
    class Deduplication {

        /**
         * The headline behaviour. Sliding 60-second windows every 30 seconds mean a three-minute
         * bunching event is measured at least six times. All six are correct; all six describe one
         * event; a dispatcher must be shown one row.
         */
        @Test
        @DisplayName("six overlapping windows of one event become one episode")
        void collapsesRepeatedWindows() {
            for (int i = 0; i < 6; i++) {
                tracker.observe(window(60 + i * 30, "SEVERE_BUNCHING", 0.22));
            }

            List<AlertEpisode> open = tracker.openEpisodes();

            assertThat(open).hasSize(1);
            AlertEpisode episode = open.get(0);
            assertThat(episode.windowsObserved()).isEqualTo(6);
            assertThat(episode.startedAt()).isEqualTo(START.plusSeconds(60));
            assertThat(episode.lastSeenAt()).isEqualTo(START.plusSeconds(210));
            assertThat(episode.duration()).isEqualTo(Duration.ofSeconds(150));
            assertThat(episode.isOpen()).isTrue();
        }

        @Test
        @DisplayName("only the first of those six is worth telling a human about")
        void onlyTheFirstIsNotable() {
            List<AlertTracker.Observation> observations = new java.util.ArrayList<>();
            for (int i = 0; i < 6; i++) {
                observations.add(tracker.observe(window(60 + i * 30, "SEVERE_BUNCHING", 0.22)));
            }

            assertThat(observations.get(0).change()).isEqualTo(AlertTracker.Change.OPENED);
            assertThat(observations.get(0).isNotable()).isTrue();
            assertThat(observations.subList(1, 6))
                    .allMatch(o -> o.change() == AlertTracker.Change.CONTINUED)
                    .allMatch(o -> !o.isNotable());

            assertThat(tracker.openedTotal()).isEqualTo(1);
            assertThat(tracker.absorbedTotal())
                    .as("five duplicate alerts suppressed").isEqualTo(5);
        }

        @Test
        @DisplayName("the episode keeps its id, so a UI can follow it across refreshes")
        void stableIdentity() {
            String first = tracker.observe(window(60, "BUNCHING", 0.4)).episode().id();
            String later = tracker.observe(window(180, "BUNCHING", 0.4)).episode().id();

            assertThat(later).isEqualTo(first);
        }
    }

    @Nested
    @DisplayName("severity")
    class Severity {

        @Test
        @DisplayName("getting worse is worth a second notification")
        void escalation() {
            tracker.observe(window(60, "BUNCHING", 0.4));

            AlertTracker.Observation worse = tracker.observe(window(90, "SEVERE_BUNCHING", 0.2));

            assertThat(worse.change()).isEqualTo(AlertTracker.Change.ESCALATED);
            assertThat(worse.isNotable()).isTrue();
            assertThat(worse.episode().worstStatus()).isEqualTo(HeadwayStatus.SEVERE_BUNCHING);
        }

        /**
         * Recovering partway is not an escalation, and the episode must not forget how bad it got:
         * an event that peaked at SEVERE_BUNCHING should not read as merely BUNCHING once it eases.
         */
        @Test
        @DisplayName("easing off keeps the worst, and reports the current, separately")
        void worstIsRemembered() {
            tracker.observe(window(60, "SEVERE_BUNCHING", 0.2));

            AlertTracker.Observation easing = tracker.observe(window(90, "BUNCHING", 0.45));

            assertThat(easing.change()).isEqualTo(AlertTracker.Change.CONTINUED);
            assertThat(easing.episode().status()).isEqualTo(HeadwayStatus.BUNCHING);
            assertThat(easing.episode().worstStatus()).isEqualTo(HeadwayStatus.SEVERE_BUNCHING);
            assertThat(easing.episode().worstRatio()).isEqualTo(0.2);
            assertThat(easing.episode().ratio()).isEqualTo(0.45);
        }

        /**
         * Bunching and gapping run in opposite directions from a ratio of 1.0, so comparing enum
         * ordinals would rank GAPPING above SEVERE_BUNCHING purely because of declaration order.
         */
        @Test
        @DisplayName("severity is not the enum's declaration order")
        void severityIsNotOrdinal() {
            assertThat(HeadwayStatus.GAPPING.ordinal())
                    .as("the trap this guards against")
                    .isGreaterThan(HeadwayStatus.SEVERE_BUNCHING.ordinal());

            tracker.observe(window(60, "SEVERE_BUNCHING", 0.2));
            AlertTracker.Observation next = tracker.observe(window(90, "GAPPING", 1.6));

            assertThat(next.episode().worstStatus()).isEqualTo(HeadwayStatus.SEVERE_BUNCHING);
            assertThat(next.change()).isNotEqualTo(AlertTracker.Change.ESCALATED);
        }

        @Test
        @DisplayName("the open list puts the worst first")
        void ordering() {
            tracker.observe(new RouteHeadway(null, START.plusSeconds(60), "1:0", "1", "One", 0,
                    null, 2, null, null, null, null, List.of(), List.of(), List.of(), List.of(),
                    600, 3000.0, 5.0, null, 1.6, "GAPPING", List.of()));
            tracker.observe(window(60, "SEVERE_BUNCHING", 0.2));

            assertThat(tracker.openEpisodes())
                    .extracting(AlertEpisode::headwayGroup)
                    .containsExactly("10:1", "1:0");
        }
    }

    @Nested
    @DisplayName("closing")
    class Closing {

        @Test
        @DisplayName("a route returning to normal closes its episode")
        void recovery() {
            tracker.observe(window(60, "SEVERE_BUNCHING", 0.2));
            tracker.observe(window(90, "SEVERE_BUNCHING", 0.2));

            AlertTracker.Observation cleared = tracker.observe(window(120, "ON_SCHEDULE", 1.0));

            assertThat(cleared.change()).isEqualTo(AlertTracker.Change.CLEARED);
            assertThat(tracker.openEpisodes()).isEmpty();
            assertThat(tracker.recentHistory(10)).hasSize(1);
            assertThat(tracker.recentHistory(10).get(0).endedAt()).isNotNull();
            assertThat(tracker.recentHistory(10).get(0).windowsObserved()).isEqualTo(2);
        }

        /**
         * LAYOVER is not alertable, so buses parked at a depot clear an episode rather than
         * sustaining one. Step 8 found exactly this on route 89.
         */
        @Test
        @DisplayName("buses going on layover clear the alert")
        void layoverClears() {
            tracker.observe(window(60, "SEVERE_BUNCHING", 0.2));

            assertThat(tracker.observe(window(90, "LAYOVER", 0.003)).change())
                    .isEqualTo(AlertTracker.Change.CLEARED);
        }

        @Test
        @DisplayName("an ON_SCHEDULE route that was never alerting changes nothing")
        void quietRouteIsSilent() {
            AlertTracker.Observation observation = tracker.observe(window(60, "ON_SCHEDULE", 1.0));

            assertThat(observation.change()).isEqualTo(AlertTracker.Change.NONE);
            assertThat(observation.episode()).isNull();
            assertThat(tracker.recentHistory(10)).isEmpty();
        }

        /**
         * A recovering route says so. A route whose buses go out of service says nothing at all,
         * and without the sweep its alert would stay on screen indefinitely.
         */
        @Test
        @DisplayName("a route that stops reporting is swept away")
        void silenceEndsAnEpisode() {
            tracker.observe(window(60, "SEVERE_BUNCHING", 0.2));

            assertThat(tracker.sweep(START.plusSeconds(120), Duration.ofMinutes(3)))
                    .as("only one minute of silence, still within the linger")
                    .isEmpty();
            assertThat(tracker.openCount()).isEqualTo(1);

            List<AlertEpisode> closed = tracker.sweep(START.plusSeconds(600), Duration.ofMinutes(3));

            assertThat(closed).hasSize(1);
            assertThat(closed.get(0).endedAt())
                    .as("the episode ended when it was last seen, not when we noticed")
                    .isEqualTo(START.plusSeconds(60));
            assertThat(tracker.openEpisodes()).isEmpty();
        }

        @Test
        @DisplayName("history is bounded")
        void boundedHistory() {
            AlertTracker small = new AlertTracker(3);
            for (int i = 0; i < 10; i++) {
                RouteHeadway bad = window(60, "BUNCHING", 0.4);
                RouteHeadway good = window(90, "ON_SCHEDULE", 1.0);
                small.observe(new RouteHeadway(bad.windowStart(), bad.windowEnd(), "r" + i + ":0",
                        "r" + i, "Route", 0, null, 2, null, null, null, null, List.of(), List.of(),
                        List.of(), List.of(), 600, 3000.0, 5.0, null, 0.4, "BUNCHING", List.of()));
                small.observe(new RouteHeadway(good.windowStart(), good.windowEnd(), "r" + i + ":0",
                        "r" + i, "Route", 0, null, 2, null, null, null, null, List.of(), List.of(),
                        List.of(), List.of(), 600, 3000.0, 5.0, null, 1.0, "ON_SCHEDULE", List.of()));
            }

            assertThat(small.recentHistory(100)).hasSize(3);
            assertThat(small.clearedTotal()).isEqualTo(10);
        }
    }

    /**
     * Windows can arrive out of order — Spark's Update mode re-emits an earlier window when late
     * data refines it. An old measurement must not drag an episode's status backwards or extend
     * its lifetime.
     */
    @Test
    @DisplayName("a late, older window is absorbed without rewriting the present")
    void lateWindowDoesNotRewriteHistory() {
        tracker.observe(window(60, "BUNCHING", 0.4));
        tracker.observe(window(120, "SEVERE_BUNCHING", 0.2));

        tracker.observe(window(90, "GAPPING", 1.8));

        AlertEpisode episode = tracker.openEpisodes().get(0);
        assertThat(episode.status()).isEqualTo(HeadwayStatus.SEVERE_BUNCHING);
        assertThat(episode.lastSeenAt()).isEqualTo(START.plusSeconds(120));
    }

    @Test
    @DisplayName("a record with no classification at all is ignored")
    void unclassifiedIsIgnored() {
        RouteHeadway noSchedule = new RouteHeadway(null, START, "9:0", "9", "Nine", 0, null,
                2, 400.0, null, null, null, List.of(), List.of(), List.of(), List.of(),
                null, null, null, null, null, null, null);

        assertThat(tracker.observe(noSchedule).change()).isEqualTo(AlertTracker.Change.NONE);
        assertThat(tracker.openCount()).isZero();
    }
}
