package dev.headway.gtfs;

import java.util.Objects;

/**
 * One row of {@code routes.txt}.
 *
 * <p><b>The id trap.</b> {@link #routeId} is the static feed's internal key — for MARTA a number
 * like {@code 26913}. The realtime feed does <em>not</em> use it. A realtime vehicle on the
 * Clifton Road bus reports {@code route_id: "15"}, which is this row's {@link #shortName}.
 *
 * <p>Joining the two feeds on {@code route_id} therefore matches <b>nothing</b>. Verified against
 * live data: nine realtime route ids matched {@code route_short_name} nine times out of nine, and
 * {@code route_id} zero times out of nine. Done as a left join it fails silently — every route
 * enriches to null and the pipeline keeps running, producing headways for routes it cannot name.
 *
 * <p>Use {@link GtfsSnapshot#routeForRealtimeId} rather than reaching for a map by {@code routeId}.
 */
public record GtfsRoute(
        String routeId,
        String shortName,
        String longName,
        int routeType,
        String colorHex) implements java.io.Serializable {

    public GtfsRoute {
        Objects.requireNonNull(routeId, "routeId");
        Objects.requireNonNull(shortName, "shortName");
    }

    /** GTFS route_type 3. MARTA's buses; the rail lines are type 1. */
    public boolean isBus() {
        return routeType == 3;
    }

    /** Something readable for a log line or a map popup. */
    public String displayName() {
        return longName == null || longName.isBlank() ? shortName : shortName + " " + longName;
    }
}
