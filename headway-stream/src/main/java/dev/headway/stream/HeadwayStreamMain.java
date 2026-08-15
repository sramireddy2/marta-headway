package dev.headway.stream;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.collect_list;
import static org.apache.spark.sql.functions.from_json;
import static org.apache.spark.sql.functions.struct;
import static org.apache.spark.sql.functions.to_json;
import static org.apache.spark.sql.functions.window;

import dev.headway.gtfs.GtfsSnapshot;
import dev.headway.gtfs.GtfsStaticRepository;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;

/**
 * Reads vehicle positions from Kafka and computes live headways per route and direction.
 *
 * <pre>
 *   Kafka vehicle-positions
 *        |  parse JSON against a declared schema
 *        v
 *   project onto the route shape          (broadcast GTFS + step 6's projector)
 *        |  drop anything more than 150 m off route
 *        v
 *   watermark 2 min, window 60s / slide 30s, group by route:direction
 *        |  newest reading per vehicle, order by distance, difference neighbours
 *        v
 *   console  +  Kafka route-headways
 * </pre>
 *
 * <h2>Watermarking, and why it is not optional</h2>
 *
 * A window covering 10:00:00–10:01:00 cannot be finalised the instant the clock passes 10:01,
 * because messages are still in flight. Some vehicle's 10:00:58 reading will arrive at 10:01:04.
 * But Spark also cannot wait forever, or the state store grows without bound.
 *
 * <p>A watermark is the answer to "how late is too late". {@code withWatermark("timestamp", "2
 * minutes")} tells Spark: once you have seen an event stamped 10:03, treat every window ending
 * before 10:01 as closed and discard its state. Anything arriving later for those windows is
 * dropped.
 *
 * <p>Two minutes is not arbitrary. Measured behaviour of this feed: MARTA republishes every ~30
 * seconds, individual vehicle timestamps run up to ~2 minutes behind the feed timestamp, and the
 * step 5 audit found one bus 708 seconds stale. Two minutes covers ordinary lateness while still
 * bounding state to a handful of windows. Set it too low and real data is silently dropped; set it
 * too high and memory grows and results are delayed. It is a latency-against-completeness dial,
 * and it is the thing to be ready to talk about.
 *
 * <h2>Why sliding windows</h2>
 *
 * 60-second windows every 30 seconds means each moment is covered by two windows and a result
 * appears twice a minute. Tumbling 60-second windows would emit half as often and a bunching event
 * straddling a boundary could be split across two windows and missed by both.
 */
public final class HeadwayStreamMain {

    /** How late an event may be and still be counted. See the class javadoc. */
    private static final String WATERMARK = "2 minutes";
    private static final String WINDOW_LENGTH = "60 seconds";
    private static final String WINDOW_SLIDE = "30 seconds";

    public static void main(String[] args) throws Exception {
        String bootstrap = env("HEADWAY_KAFKA_BOOTSTRAP", "localhost:9092");
        String sourceTopic = env("HEADWAY_KAFKA_TOPIC", "vehicle-positions");
        String sinkTopic = env("HEADWAY_HEADWAY_TOPIC", "route-headways");
        String startingOffsets = env("HEADWAY_STARTING_OFFSETS", "latest");
        // Configurable because the working directory differs between a local run and the
        // container, where these are bind-mounted at /data.
        Path gtfsDir = Path.of(env("HEADWAY_GTFS_DIR", "data/gtfs"));
        Path checkpointDir = Path.of(env("HEADWAY_CHECKPOINT_DIR", "data/spark-checkpoint"));

        SparkSession spark = SparkSession.builder()
                .appName("headway-stream")
                .master(env("HEADWAY_SPARK_MASTER", "local[*]"))
                .config("spark.ui.enabled", "false")
                // Default is 200 shuffle partitions. For ~65 route-direction groups on one laptop
                // that means 200 mostly-empty tasks per batch, and the scheduling overhead
                // dominates the actual work. This is the single most common local-Spark slowdown.
                .config("spark.sql.shuffle.partitions", "8")
                .config("spark.sql.session.timeZone", "UTC")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");

        try (GtfsStaticRepository repository = GtfsStaticRepository.marta(gtfsDir)) {
            GtfsSnapshot snapshot = repository.snapshot();
            System.out.println("Loaded " + snapshot);

            // Ship the schedule to every executor once, rather than with every task.
            Broadcast<GtfsSnapshot> gtfs =
                    spark.sparkContext().broadcast(snapshot,
                            scala.reflect.ClassTag$.MODULE$.apply(GtfsSnapshot.class));

            UserDefinedFunction project = HeadwayFunctions.projectUdf(gtfs);
            UserDefinedFunction gaps = HeadwayFunctions.gapsUdf();

            Dataset<Row> raw = spark.readStream()
                    .format("kafka")
                    .option("kafka.bootstrap.servers", bootstrap)
                    .option("subscribe", sourceTopic)
                    .option("startingOffsets", startingOffsets)
                    // Without this a backlog is replayed as one enormous first batch, which stalls
                    // the query and makes the first window meaningless.
                    .option("maxOffsetsPerTrigger", "20000")
                    .load();

            Dataset<Row> positions = raw
                    .select(from_json(col("value").cast("string"),
                            HeadwaySchema.VEHICLE_POSITION).as("p"))
                    .select("p.*")
                    .filter(col("vehicleId").isNotNull().and(col("tripId").isNotNull()));

            Dataset<Row> projected = positions
                    .withColumn("proj", project.apply(
                            col("tripId"), col("latitude"), col("longitude")))
                    .filter(col("proj").isNotNull())
                    .select(
                            col("vehicleId"),
                            col("timestamp").as("ts"),
                            col("proj.headwayGroup").as("headwayGroup"),
                            col("proj.routeShortName").as("routeShortName"),
                            col("proj.routeLongName").as("routeLongName"),
                            col("proj.directionId").as("directionId"),
                            col("proj.shapeLengthMetres").as("shapeLengthMetres"),
                            col("proj.distanceAlongRouteMetres").as("distanceMetres"),
                            col("proj.crossTrackMetres").as("crossTrackMetres"))
                    // Deadheading buses are far from the route they are assigned to; including
                    // them would invent gaps between buses that are not actually running it.
                    .filter(col("crossTrackMetres").leq(HeadwayFunctions.MAX_CROSS_TRACK_METRES));

            Dataset<Row> headways = projected
                    .withWatermark("ts", WATERMARK)
                    .groupBy(
                            window(col("ts"), WINDOW_LENGTH, WINDOW_SLIDE).as("w"),
                            col("headwayGroup"),
                            col("routeShortName"),
                            col("routeLongName"),
                            col("directionId"))
                    .agg(collect_list(struct(
                            col("vehicleId"), col("distanceMetres"), col("ts"))).as("sightings"))
                    .withColumn("h", gaps.apply(col("sightings")))
                    .select(
                            col("w.start").as("windowStart"),
                            col("w.end").as("windowEnd"),
                            col("headwayGroup"),
                            col("routeShortName"),
                            col("routeLongName"),
                            col("directionId"),
                            col("h.vehicleCount").as("vehicleCount"),
                            col("h.minGapMetres").as("minGapMetres"),
                            col("h.medianGapMetres").as("medianGapMetres"),
                            col("h.maxGapMetres").as("maxGapMetres"),
                            col("h.meanGapMetres").as("meanGapMetres"),
                            col("h.gapsMetres").as("gapsMetres"),
                            col("h.orderedVehicles").as("orderedVehicles"),
                            col("h.orderedDistancesMetres").as("orderedDistancesMetres"))
                    // A single bus has nobody to have a gap with.
                    .filter(col("vehicleCount").geq(2));

            String checkpointRoot = checkpointDir.toAbsolutePath().toUri().toString();

            // Update mode, not Append. Append withholds a window until the watermark has passed
            // it, so nothing at all appears for the first few minutes - useless for a live
            // dashboard. Update emits each window as it changes, so results show up immediately
            // and get refined as more data lands.
            //
            // Everything is written from ONE query via foreachBatch, which hands you an ordinary
            // batch DataFrame. Three reasons that beats two separate writeStream calls:
            //
            //  1. Two queries would each open their own Kafka consumer and read the source topic
            //     independently - twice the work for one pipeline.
            //  2. Streaming aggregations cannot be sorted; orderBy needs Complete mode, and
            //     Complete mode retains every window forever, which quietly defeats the whole
            //     point of the watermark. A batch DataFrame sorts freely.
            //  3. The console output and the Kafka records are then guaranteed to be the same
            //     rows, rather than two queries drifting a batch apart.
            StreamingQuery query = headways
                    .writeStream()
                    .outputMode(OutputMode.Update())
                    .foreachBatch((VoidFunction2<Dataset<Row>, Long>) (batch, batchId) -> {
                        // The batch is consumed twice below. Without persist() Spark recomputes
                        // the entire aggregation for each action - including re-running the
                        // projection UDF over every row.
                        batch.persist();
                        try {
                            long groups = batch.count();
                            System.out.printf("%n=== batch %d: %d route-direction groups with 2+ buses ===%n",
                                    batchId, groups);
                            if (groups > 0) {
                                batch.select(col("windowEnd"), col("headwayGroup"),
                                                col("routeLongName"), col("vehicleCount"),
                                                col("minGapMetres"), col("medianGapMetres"),
                                                col("maxGapMetres"))
                                        .orderBy(col("minGapMetres"))
                                        .show(12, false);

                                batch.select(col("headwayGroup").as("key"),
                                                to_json(struct(col("*"))).as("value"))
                                        .write()
                                        .format("kafka")
                                        .option("kafka.bootstrap.servers", bootstrap)
                                        .option("topic", sinkTopic)
                                        .save();
                            }
                        } finally {
                            batch.unpersist();
                        }
                    })
                    .trigger(Trigger.ProcessingTime(30, TimeUnit.SECONDS))
                    .option("checkpointLocation", checkpointRoot + "/headways")
                    .start();

            System.out.println("Streaming. Source '" + sourceTopic + "' -> sink '" + sinkTopic
                    + "'. Ctrl+C to stop.");
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                // StreamingQuery.stop() declares TimeoutException. During shutdown there is
                // nothing useful to do about it beyond carrying on and letting the JVM exit.
                stopQuietly(query);
                spark.stop();
            }, "headway-stream-shutdown"));

            query.awaitTermination();
        }
    }

    private static void stopQuietly(StreamingQuery query) {
        try {
            query.stop();
        } catch (Exception e) {
            System.err.println("Could not stop query " + query.name() + ": " + e);
        }
    }

    private static String env(String name, String fallback) {
        Map<String, String> environment = System.getenv();
        String value = environment.get(name);
        return value == null || value.isBlank() ? fallback : value;
    }

    private HeadwayStreamMain() {}
}
