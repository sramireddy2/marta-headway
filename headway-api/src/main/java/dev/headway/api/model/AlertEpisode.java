package dev.headway.api.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import dev.headway.gtfs.HeadwayStatus;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * One continuous stretch of time during which a route was bunched or gapped.
 *
 * <h2>The problem this type exists to solve</h2>
 *
 * The stream job uses 60-second windows sliding every 30 seconds, so every moment is covered by two
 * windows, and a bunching event that lasts three minutes is reported six or more times. Those
 * reports are all <em>correct</em> — they are six independent measurements — but they describe one
 * event, and an alert feed that emits six rows for one event is one a dispatcher stops reading.
 *
 * <p>This is the limitation step 8 documented and did not fix, and it could not be fixed in Spark:
 * a streaming aggregation has no memory across windows by design, which is exactly what makes it
 * scale. Collapsing repeats needs a small piece of durable state keyed by route, and here — one
 * process, holding a map, reading a topic keyed by that same route — is where that state belongs.
 *
 * <p>So an episode <b>opens</b> the first time a group reports an alertable status, <b>absorbs</b>
 * every subsequent window that agrees, and <b>closes</b> when the group returns to normal or stops
 * reporting. Six window records become one row with a start time, a duration, and the worst it got.
 *
 * @param id stable for the life of the episode, so a UI can track it across refreshes
 * @param status what the most recent window said
 * @param worstStatus the worst seen at any point, which is what a dispatcher is triaging on
 * @param worstRatio the ratio at that worst moment
 * @param windowsObserved how many window records were folded into this one row
 * @param endedAt null while the episode is still open
 */
public record AlertEpisode(
        String id,
        String headwayGroup,
        String routeShortName,
        String routeLongName,
        HeadwayStatus status,
        HeadwayStatus worstStatus,
        Double ratio,
        Double worstRatio,
        Double gapMetres,
        Double expectedSpacingMetres,
        Integer scheduledHeadwaySeconds,
        List<String> vehicles,
        Instant startedAt,
        Instant lastSeenAt,
        Instant endedAt,
        int windowsObserved) {

    /** Opens a new episode from the window that first reported trouble. */
    public static AlertEpisode opening(RouteHeadway headway) {
        HeadwayStatus status = headway.statusEnum();
        Instant at = headway.windowEnd();
        return new AlertEpisode(
                headway.headwayGroup() + "@" + at.toEpochMilli(),
                headway.headwayGroup(),
                headway.routeShortName(),
                headway.routeLongName(),
                status,
                status,
                headway.headwayRatio(),
                headway.headwayRatio(),
                headway.minGapMetres(),
                headway.expectedSpacingMetres(),
                headway.scheduledHeadwaySeconds(),
                headway.worstPairVehicles() == null ? List.of() : List.copyOf(headway.worstPairVehicles()),
                at,
                at,
                null,
                1);
    }

    /**
     * Folds one more agreeing window into this episode.
     *
     * <p>"Worst" is tracked separately from "current" because they answer different questions. A
     * dispatcher triaging a list wants the worst it got; someone looking at one route wants to know
     * whether it is recovering. Keeping only the latest would make an episode that peaked at
     * SEVERE_BUNCHING and eased to BUNCHING look like it had never been serious.
     */
    public AlertEpisode absorb(RouteHeadway headway) {
        HeadwayStatus incoming = headway.statusEnum();
        boolean worse = isWorse(incoming, headway.headwayRatio(), worstStatus, worstRatio);
        return new AlertEpisode(
                id,
                headwayGroup,
                headway.routeShortName() != null ? headway.routeShortName() : routeShortName,
                headway.routeLongName() != null ? headway.routeLongName() : routeLongName,
                incoming,
                worse ? incoming : worstStatus,
                headway.headwayRatio(),
                worse ? headway.headwayRatio() : worstRatio,
                headway.minGapMetres(),
                headway.expectedSpacingMetres() != null
                        ? headway.expectedSpacingMetres() : expectedSpacingMetres,
                headway.scheduledHeadwaySeconds() != null
                        ? headway.scheduledHeadwaySeconds() : scheduledHeadwaySeconds,
                headway.worstPairVehicles() == null ? vehicles : List.copyOf(headway.worstPairVehicles()),
                startedAt,
                // max, not assignment: windows can arrive out of order, and an episode's
                // lastSeenAt drives when it is swept away as finished.
                headway.windowEnd().isAfter(lastSeenAt) ? headway.windowEnd() : lastSeenAt,
                null,
                windowsObserved + 1);
    }

    public AlertEpisode closing(Instant at) {
        return new AlertEpisode(id, headwayGroup, routeShortName, routeLongName, status, worstStatus,
                ratio, worstRatio, gapMetres, expectedSpacingMetres, scheduledHeadwaySeconds,
                vehicles, startedAt, lastSeenAt, at, windowsObserved);
    }

    @JsonIgnore
    public boolean isOpen() {
        return endedAt == null;
    }

    /**
     * How long the event has been running, or ran for.
     *
     * <p>Exposed as seconds rather than a {@link Duration} because this record is serialised by two
     * different mappers — Spring's for HTTP and {@code Json}'s for the WebSocket — and they
     * disagree about durations by default: one writes {@code "PT3M"}, the other {@code 180.000}.
     * A field whose type depends on which door it came out of is a trap for the front end. A plain
     * number cannot be rendered two ways.
     *
     * <p>The annotation is not decorative. Jackson auto-detects only bean-style accessors —
     * {@code getDurationSeconds()} — and record components; a method named {@code
     * durationSeconds()} on a record is neither, so without the annotation this simply never
     * appears in the JSON. That absence was found by reading live output, not by the tests,
     * because the round-trip test recomputed the value from {@code startedAt} after parsing and so
     * never noticed the field was missing from the wire.
     */
    @com.fasterxml.jackson.annotation.JsonProperty("durationSeconds")
    public long durationSeconds() {
        return duration().toSeconds();
    }

    @JsonIgnore
    public Duration duration() {
        Instant end = endedAt != null ? endedAt : lastSeenAt;
        return Duration.between(startedAt, end);
    }

    /**
     * Is {@code candidate} a more serious reading than {@code incumbent}?
     *
     * <p>Severity is not a linear scale over the enum: bunching and gapping run in opposite
     * directions from ON_SCHEDULE. Comparing the ordinals would say GAPPING is worse than
     * SEVERE_BUNCHING purely because of declaration order. So this compares severity ranks and
     * falls back to distance from a ratio of 1.0.
     */
    static boolean isWorse(HeadwayStatus candidate, Double candidateRatio,
            HeadwayStatus incumbent, Double incumbentRatio) {
        int candidateRank = severity(candidate);
        int incumbentRank = severity(incumbent);
        if (candidateRank != incumbentRank) {
            return candidateRank > incumbentRank;
        }
        if (candidateRatio == null || incumbentRatio == null) {
            return false;
        }
        return Math.abs(candidateRatio - 1.0) > Math.abs(incumbentRatio - 1.0);
    }

    private static int severity(HeadwayStatus status) {
        if (status == null) {
            return 0;
        }
        return switch (status) {
            case SEVERE_BUNCHING, SEVERE_GAPPING -> 2;
            case BUNCHING, GAPPING -> 1;
            case ON_SCHEDULE, LAYOVER -> 0;
        };
    }
}
