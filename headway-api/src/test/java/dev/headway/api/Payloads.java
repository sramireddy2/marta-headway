package dev.headway.api;

import java.time.Instant;

/**
 * Sample records in exactly the shape the Spark job writes.
 *
 * <p>Built as raw JSON strings rather than by serialising a {@code RouteHeadway}. Serialising the
 * class under test and then parsing it back proves only that Jackson is symmetric — it would still
 * pass if every field name here disagreed with what Spark emits, which is the one failure that
 * matters. These strings were taken from the shape of {@code HeadwaySchema.HEADWAYS} plus the
 * window columns {@code HeadwayStreamMain} adds.
 */
public final class Payloads {

    private Payloads() {}

    /** A route-headway record. {@code status} and {@code ratio} drive the classification tests. */
    public static String headway(String group, Instant windowEnd, String status, double ratio,
            double minGapMetres) {
        return """
                {"windowStart":"%s","windowEnd":"%s","headwayGroup":"%s","routeShortName":"%s",\
                "routeLongName":"AUC / Hollywood Road","directionId":1,"routeLengthMetres":24000.5,\
                "vehicleCount":4,"minGapMetres":%s,"medianGapMetres":5000.0,"maxGapMetres":9000.0,\
                "meanGapMetres":5571.0,"gapsMetres":[%s,5000.0,9000.0],\
                "orderedVehicles":["4663","4671","4680","4690"],\
                "orderedDistancesMetres":[100.0,2813.0,7813.0,16813.0],"layoverVehicles":[],\
                "scheduledHeadwaySeconds":1800,"expectedSpacingMetres":12503.0,\
                "averageSpeedMps":6.95,"observedHeadwaySeconds":390.5,"headwayRatio":%s,\
                "status":"%s","worstPairVehicles":["4663","4671"]}"""
                .formatted(windowEnd.minusSeconds(60), windowEnd, group, group.split(":")[0],
                        minGapMetres, minGapMetres, ratio, status);
    }

    /**
     * A record from a route the timetable has nothing to say about.
     *
     * <p>Spark's {@code to_json} <em>omits</em> null fields rather than writing them as null, so
     * this is genuinely a shorter object, not the same object with nulls in it. Any consumer that
     * assumed the keys were always present would break on it.
     */
    public static String headwayWithoutSchedule(String group, Instant windowEnd) {
        return """
                {"windowStart":"%s","windowEnd":"%s","headwayGroup":"%s","routeShortName":"%s",\
                "routeLongName":"Unscheduled Shuttle","directionId":0,"routeLengthMetres":8000.0,\
                "vehicleCount":2,"minGapMetres":400.0,"medianGapMetres":400.0,"maxGapMetres":400.0,\
                "meanGapMetres":400.0,"gapsMetres":[400.0],"orderedVehicles":["1","2"],\
                "orderedDistancesMetres":[100.0,500.0],"layoverVehicles":[]}"""
                .formatted(windowEnd.minusSeconds(60), windowEnd, group, group.split(":")[0]);
    }

    /** As written by the ingest service's {@code VehiclePositionSerializer}. */
    public static String vehicle(String vehicleId, String routeId, Instant at) {
        return """
                {"vehicleId":"%s","routeId":"%s","tripId":"t-%s","directionId":5,\
                "latitude":33.7490,"longitude":-84.3880,"bearingDegrees":180.0,\
                "speedMetersPerSecond":8.5,"timestamp":"%s"}"""
                .formatted(vehicleId, routeId, vehicleId, at);
    }
}
