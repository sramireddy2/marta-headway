package dev.headway.gtfs;

import java.util.Objects;

/**
 * One row of {@code trips.txt} — a single scheduled run along a route.
 *
 * <p>This record is how the {@code direction_id} problem from step 3 gets solved. MARTA's
 * <em>realtime</em> feed puts values like 5, 9, 11, 14 and 17 in {@code direction_id}, none of
 * which are the GTFS 0/1 outbound/inbound flag. The <em>static</em> feed is correct: 26,549 trips
 * with direction 0 and 25,852 with direction 1, and nothing else.
 *
 * <p>So the realtime {@code trip_id} is looked up here, and {@link #directionId} is the answer.
 * That matters because headway is only meaningful between buses travelling the same way — a
 * northbound and a southbound bus passing each other are not consecutive, and mixing them would
 * invent bunching that is not happening.
 */
public record GtfsTrip(
        String tripId,
        String routeId,
        int directionId,
        String shapeId,
        String headsign) implements java.io.Serializable {

    public GtfsTrip {
        Objects.requireNonNull(tripId, "tripId");
        Objects.requireNonNull(routeId, "routeId");
        if (directionId != 0 && directionId != 1) {
            throw new IllegalArgumentException(
                    "direction_id must be 0 or 1 per the GTFS spec, got " + directionId
                            + " for trip " + tripId);
        }
    }

    /** True for direction 0. The GTFS spec leaves the meaning to the agency; it is only a pair. */
    public boolean isOutbound() {
        return directionId == 0;
    }
}
