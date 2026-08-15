package dev.headway.stream;

import dev.headway.gtfs.GtfsSnapshot;
import dev.headway.gtfs.HeadwayStatus;
import dev.headway.gtfs.ScheduleIndex;
import dev.headway.gtfs.ShapeProjection;
import dev.headway.gtfs.ShapeProjector;
import dev.headway.gtfs.TripContext;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.api.java.UDF4;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.functions;
import scala.collection.JavaConverters;

/**
 * The two user-defined functions the streaming job is built around.
 *
 * <h2>Broadcasting the schedule</h2>
 *
 * The projection needs the shape file, which is ~8.6 MB of coordinates. Referencing it from a
 * lambda would serialise a fresh copy into <em>every task</em> — hundreds of copies per batch. A
 * {@link Broadcast} ships it to each executor once and every task there reads the same instance.
 * That is what broadcast variables are for, and a shape file is close to the textbook case: large,
 * read-only, and needed by every row.
 *
 * <p>The GTFS classes were made {@link java.io.Serializable} for exactly this, so the job reuses
 * the {@code ShapeProjector} that step 6 tested rather than reimplementing the geometry in Spark.
 * Two implementations of the same maths is two things to keep correct.
 */
final class HeadwayFunctions {

    /** Beyond this the fix is not on the route we think it is — usually a deadheading bus. */
    static final double MAX_CROSS_TRACK_METRES = 150.0;

    /**
     * GTFS schedule times are in the agency's local time, not UTC.
     *
     * <p>Looking up an 18:30 Atlanta schedule with the UTC hour would read the 22:30 timetable —
     * comparing a rush-hour service against a late-evening one, and reporting bunching on a route
     * that is running exactly as planned. Silent, seasonal, and four hours wrong.
     */
    static final ZoneId AGENCY_ZONE = ZoneId.of("America/New_York");

    private HeadwayFunctions() {}

    /**
     * Resolves a vehicle against the schedule and projects it onto its route's path.
     *
     * <p>Returns a struct rather than a scalar so one call produces every derived field. Four
     * separate UDFs would mean projecting the same point four times.
     *
     * <p>Note this uses the unhinted {@link ShapeProjector#project}: a UDF is a pure function of
     * its row and has no memory of where the vehicle was last seen. Step 6 measured the cost —
     * about 0.2% of positions on self-intersecting shapes get an ambiguous answer. Carrying a
     * per-vehicle hint would need arbitrary stateful processing, which is a bigger change than the
     * error justifies today; the windowed median below is chosen partly because it tolerates the
     * occasional outlier.
     */
    static UserDefinedFunction projectUdf(Broadcast<GtfsSnapshot> gtfs) {
        UDF3<String, Double, Double, Row> fn = (tripId, latitude, longitude) -> {
            if (tripId == null || latitude == null || longitude == null) {
                return null;
            }
            Optional<TripContext> resolved = gtfs.value().resolve(null, tripId);
            if (resolved.isEmpty()) {
                return null;
            }
            TripContext context = resolved.get();
            ShapeProjection projection =
                    ShapeProjector.project(context.shape(), latitude, longitude);

            return RowFactory.create(
                    context.headwayGroup(),
                    context.route().shortName(),
                    context.route().longName(),
                    context.trip().directionId(),
                    context.shape().shapeId(),
                    context.shape().lengthMetres(),
                    projection.distanceAlongRouteMetres(),
                    projection.crossTrackMetres());
        };
        return functions.udf(fn, HeadwaySchema.PROJECTION);
    }

    /**
     * Turns a window's worth of sightings into the gaps between consecutive buses.
     *
     * <p>Three things happen here, and the order matters:
     *
     * <ol>
     *   <li><b>Deduplicate to one reading per vehicle.</b> A 60-second window at a 15-second poll
     *       contains roughly four sightings of the same bus. Leaving them in would produce
     *       "gaps" of a few metres between a bus and itself — the system would report constant
     *       bunching. Keeping only each vehicle's newest reading is the fix.
     *   <li><b>Sort by distance along the route.</b> Not by vehicle id, not by arrival order.
     *       Position along the route is the only ordering in which "consecutive" means anything.
     *   <li><b>Difference adjacent pairs.</b> Those differences are the headways.
     * </ol>
     *
     * <p>Returns the ordered vehicles and distances alongside the statistics so an alert can name
     * which two buses are too close, not merely that some pair is.
     */
    /**
     * Computes the gaps and classifies them against the timetable.
     *
     * <p>Takes the route length so terminal layovers can be excluded, and the window's end time so
     * the correct scheduled headway is used — a route running every 5 minutes at 08:00 may run
     * every 40 at 22:00, and comparing against the wrong one inverts the answer.
     */
    static UserDefinedFunction gapsUdf(Broadcast<GtfsSnapshot> gtfs) {
        UDF4<Object, String, Double, java.sql.Timestamp, Row> fn =
                (sightings, headwayGroup, routeLengthMetres, windowEnd) -> {

            List<HeadwayGaps.Sighting> parsed = new ArrayList<>();
            for (Row row : toRowList(sightings)) {
                parsed.add(new HeadwayGaps.Sighting(
                        row.getAs("vehicleId"),
                        row.<Double>getAs("distanceMetres"),
                        timestampOf(row)));
            }

            double length = routeLengthMetres == null ? Double.NaN : routeLengthMetres;
            HeadwayGaps.Result result = HeadwayGaps.compute(parsed, length);

            // Everything below is the schedule comparison; all of it may be absent.
            Integer scheduledSeconds = null;
            Double expectedSpacing = null;
            Double speed = null;
            Double observedSeconds = null;
            Double ratio = null;
            String status = null;
            String[] worstPair = null;

            if (!result.gapsMetres().isEmpty() && windowEnd != null) {
                ZonedDateTime at = windowEnd.toInstant().atZone(AGENCY_ZONE);
                Optional<ScheduleIndex.Scheduled> scheduled = gtfs.value().schedule()
                        .scheduledFor(headwayGroup, at.toLocalDate(),
                                at.toLocalTime().toSecondOfDay());

                if (scheduled.isPresent()) {
                    ScheduleIndex.Scheduled s = scheduled.get();
                    scheduledSeconds = s.headwaySeconds();
                    speed = s.averageSpeedMps();
                    expectedSpacing = s.expectedSpacingMetres();

                    // The tightest gap is what a dispatcher acts on: one bunched pair is a problem
                    // even when every other pair on the route is perfectly spaced.
                    double worstGap = result.minGapMetres();
                    observedSeconds = worstGap / s.averageSpeedMps();
                    ratio = expectedSpacing > 0 ? worstGap / expectedSpacing : null;

                    if (ratio != null) {
                        status = HeadwayStatus.fromRatio(ratio).name();
                        int index = result.gapsMetres().indexOf(worstGap);
                        if (index >= 0 && index + 1 < result.orderedVehicles().size()) {
                            worstPair = new String[] {
                                    result.orderedVehicles().get(index),
                                    result.orderedVehicles().get(index + 1)};
                        }
                    }
                }
            }

            if (status == null && !result.layoverVehicles().isEmpty()
                    && result.vehicleCount() < 2) {
                status = HeadwayStatus.LAYOVER.name();
            }

            return RowFactory.create(
                    result.vehicleCount(),
                    result.gapsMetres().toArray(new Double[0]),
                    result.minGapMetres(),
                    result.medianGapMetres(),
                    result.maxGapMetres(),
                    result.meanGapMetres(),
                    result.orderedVehicles().toArray(new String[0]),
                    result.orderedDistancesMetres().toArray(new Double[0]),
                    result.layoverVehicles().toArray(new String[0]),
                    scheduledSeconds, expectedSpacing, speed,
                    observedSeconds, ratio, status, worstPair);
        };
        return functions.udf(fn, HeadwaySchema.HEADWAYS);
    }

    private static long timestampOf(Row row) {
        java.sql.Timestamp ts = row.getAs("ts");
        return ts == null ? 0L : ts.getTime();
    }

    /**
     * Spark hands array columns to a Java UDF as a Scala sequence, not a {@link List}.
     *
     * <p>This is one of the sharper edges of writing Spark UDFs in Java rather than Scala: the
     * parameter type has to be {@code Object} and converted here, because declaring
     * {@code List<Row>} produces a {@link ClassCastException} at runtime rather than a compile
     * error.
     */
    @SuppressWarnings("unchecked")
    private static List<Row> toRowList(Object sightings) {
        if (sightings instanceof List<?> list) {
            return (List<Row>) list;
        }
        if (sightings instanceof scala.collection.Seq<?> seq) {
            return (List<Row>) JavaConverters.seqAsJavaList(seq);
        }
        throw new IllegalArgumentException(
                "unexpected array representation: " + sightings.getClass());
    }
}
