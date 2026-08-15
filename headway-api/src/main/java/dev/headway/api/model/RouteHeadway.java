package dev.headway.api.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import dev.headway.gtfs.HeadwayStatus;
import java.time.Instant;
import java.util.List;

/**
 * One route-direction group as the Spark job measured it in one window.
 *
 * <p>This mirrors the JSON written to the {@code route-headways} topic by
 * {@code HeadwayStreamMain}, which builds it with {@code to_json(struct(col("*")))}. That means the
 * field names here are a <b>contract with a program in another module and another language
 * runtime</b>, not just a DTO — rename a column in the Spark job and the matching field here goes
 * null, silently.
 *
 * <p>Two consequences shape the code below:
 *
 * <ul>
 *   <li>Every numeric field that Spark can emit as null is boxed. Spark's {@code to_json} drops
 *       null fields entirely rather than writing {@code "field": null}, so a primitive {@code
 *       double} would not merely be zero — Jackson would leave it at its default and nothing would
 *       indicate the value was absent. {@code scheduledHeadwaySeconds} is null for every route the
 *       timetable has nothing to say about, which is a normal case, not an error.
 *   <li>Timestamps are read leniently. See {@link LenientInstantDeserializer}.
 * </ul>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record RouteHeadway(
        @JsonDeserialize(using = LenientInstantDeserializer.class) Instant windowStart,
        @JsonDeserialize(using = LenientInstantDeserializer.class) Instant windowEnd,
        String headwayGroup,
        String routeShortName,
        String routeLongName,
        Integer directionId,
        Double routeLengthMetres,
        int vehicleCount,
        Double minGapMetres,
        Double medianGapMetres,
        Double maxGapMetres,
        Double meanGapMetres,
        List<Double> gapsMetres,
        List<String> orderedVehicles,
        List<Double> orderedDistancesMetres,
        List<String> layoverVehicles,
        Integer scheduledHeadwaySeconds,
        Double expectedSpacingMetres,
        Double averageSpeedMps,
        Double observedHeadwaySeconds,
        Double headwayRatio,
        String status,
        List<String> worstPairVehicles) {

    /**
     * The status as an enum rather than a string, or null if the stream job could not classify it.
     *
     * <p>Parsed defensively. An unrecognised value means the two sides have been deployed at
     * different versions, and the honest answer to "what does UNKNOWN_STATUS mean" is null rather
     * than a guess or an exception that kills the consumer thread.
     */
    @JsonIgnore
    public HeadwayStatus statusEnum() {
        if (status == null) {
            return null;
        }
        try {
            return HeadwayStatus.valueOf(status);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    @JsonIgnore
    public boolean isAlertable() {
        HeadwayStatus parsed = statusEnum();
        return parsed != null && parsed.isAlertable();
    }

    /** Best available label for a human: the long name, falling back to the route number. */
    @JsonIgnore
    public String displayName() {
        if (routeLongName != null && !routeLongName.isBlank()) {
            return routeLongName;
        }
        return routeShortName != null ? routeShortName : headwayGroup;
    }
}
