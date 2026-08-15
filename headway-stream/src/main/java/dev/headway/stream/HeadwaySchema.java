package dev.headway.stream;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/**
 * The shapes of the data moving through the streaming job.
 *
 * <h2>Why the input schema is written out rather than inferred</h2>
 *
 * Spark can infer a JSON schema by sampling, and for a streaming source that is a bad idea. The
 * sample is whatever happened to arrive first, so a field that is null in the first batch gets
 * typed from the second batch instead — or not at all. Worse, the inferred schema can differ
 * between restarts, which quietly invalidates the checkpoint.
 *
 * <p>Declaring it makes the contract with {@code headway-common}'s {@code Json} explicit: these
 * field names and types are what the producer writes. If someone renames a field on the ingest
 * side, the column here goes null and the mismatch is visible, rather than the job crashing
 * somewhere unrelated.
 */
final class HeadwaySchema {

    /** Matches {@code dev.headway.common.VehiclePosition} as serialised by {@code Json}. */
    static final StructType VEHICLE_POSITION = new StructType()
            .add("vehicleId", DataTypes.StringType, false)
            .add("routeId", DataTypes.StringType, false)
            .add("tripId", DataTypes.StringType, true)
            .add("directionId", DataTypes.IntegerType, true)
            .add("latitude", DataTypes.DoubleType, false)
            .add("longitude", DataTypes.DoubleType, false)
            .add("bearingDegrees", DataTypes.DoubleType, true)
            .add("speedMetersPerSecond", DataTypes.DoubleType, true)
            // ISO-8601 with a Z suffix, because Json disables WRITE_DATES_AS_TIMESTAMPS.
            .add("timestamp", DataTypes.TimestampType, false);

    /** What the projection UDF returns for one vehicle. */
    static final StructType PROJECTION = new StructType()
            .add("headwayGroup", DataTypes.StringType, true)
            .add("routeShortName", DataTypes.StringType, true)
            .add("routeLongName", DataTypes.StringType, true)
            .add("directionId", DataTypes.IntegerType, true)
            .add("shapeId", DataTypes.StringType, true)
            .add("shapeLengthMetres", DataTypes.DoubleType, true)
            .add("distanceAlongRouteMetres", DataTypes.DoubleType, true)
            .add("crossTrackMetres", DataTypes.DoubleType, true);

    /** What the gap UDF returns for one route-direction group in one window. */
    static final StructType HEADWAYS = new StructType()
            .add("vehicleCount", DataTypes.IntegerType, false)
            .add("gapsMetres", DataTypes.createArrayType(DataTypes.DoubleType), false)
            .add("minGapMetres", DataTypes.DoubleType, true)
            .add("medianGapMetres", DataTypes.DoubleType, true)
            .add("maxGapMetres", DataTypes.DoubleType, true)
            .add("meanGapMetres", DataTypes.DoubleType, true)
            .add("orderedVehicles", DataTypes.createArrayType(DataTypes.StringType), false)
            .add("orderedDistancesMetres", DataTypes.createArrayType(DataTypes.DoubleType), false);

    private HeadwaySchema() {}
}
