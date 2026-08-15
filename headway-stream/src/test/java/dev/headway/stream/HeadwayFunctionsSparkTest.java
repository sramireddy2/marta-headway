package dev.headway.stream;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.collect_list;
import static org.apache.spark.sql.functions.struct;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import dev.headway.gtfs.GtfsSnapshot;
import dev.headway.gtfs.GtfsStaticLoader;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Exercises the UDFs through a real Spark DataFrame.
 *
 * <p>{@link HeadwayGapsTest} covers the arithmetic; this covers everything between that arithmetic
 * and Spark — the schema, Catalyst {@link Row} conversion, broadcasting the schedule, and the
 * Scala-collection edge where an array column arrives as a {@code scala.collection.Seq} rather
 * than a {@link List}. That last one is a {@link ClassCastException} at runtime, never a compile
 * error, so it needs a real DataFrame to catch.
 *
 * <p>Batch, not streaming. Streaming would need a checkpoint directory, and checkpointing is
 * precisely what does not work on Windows without Hadoop's native binaries — the reason the job
 * itself runs in Docker. Batch Spark works fine here, and the UDFs cannot tell the difference.
 */
class HeadwayFunctionsSparkTest {

    private static SparkSession spark;
    private static Broadcast<GtfsSnapshot> gtfs;
    private static UserDefinedFunction projectUdf;
    private static UserDefinedFunction gapsUdf;

    /** Two points 0.001 degrees of latitude apart are 111.195 m; SHAPE_A runs due north. */
    private static final double BASE_LAT = 33.750000;
    private static final double BASE_LON = -84.390000;

    @BeforeAll
    static void startSpark(@org.junit.jupiter.api.io.TempDir Path temp) throws IOException {
        spark = SparkSession.builder()
                .appName("headway-functions-test")
                .master("local[2]")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.shuffle.partitions", "2")
                .config("spark.sql.session.timeZone", "UTC")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        GtfsSnapshot snapshot = new GtfsStaticLoader()
                .load(writeTestFeed(temp), Instant.parse("2026-06-24T16:40:00Z"));
        gtfs = spark.sparkContext().broadcast(snapshot,
                scala.reflect.ClassTag$.MODULE$.apply(GtfsSnapshot.class));

        projectUdf = HeadwayFunctions.projectUdf(gtfs);
        gapsUdf = HeadwayFunctions.gapsUdf(gtfs);
    }

    @AfterAll
    static void stopSpark() {
        if (spark != null) {
            spark.stop();
        }
    }

    /** A minimal GTFS archive: one route, two directions, one due-north shape 1 km long. */
    private static Path writeTestFeed(Path directory) throws IOException {
        StringBuilder shapes = new StringBuilder(
                "shape_id,shape_pt_lat,shape_pt_lon,shape_pt_sequence,shape_dist_traveled\n");
        for (int i = 0; i <= 10; i++) {
            shapes.append("SHAPE_A,")
                    .append(BASE_LAT + i * 0.001).append(',')
                    .append(BASE_LON).append(',')
                    .append(i + 1).append(',')
                    .append(i * 0.111195)  // kilometres, as MARTA publishes
                    .append('\n');
        }

        Map<String, String> files = Map.of(
                "routes.txt",
                "route_id,route_short_name,route_long_name,route_type,route_color\n"
                        + "26913,15,Clifton Road,3,367AA8\n",
                "trips.txt",
                "route_id,service_id,trip_id,trip_headsign,direction_id,shape_id\n"
                        + "26913,1,T-OUT,Candler Road,0,SHAPE_A\n"
                        + "26913,1,T-IN,Downtown,1,SHAPE_A\n",
                "shapes.txt", shapes.toString());

        Path zip = directory.resolve("google_transit.zip");
        try (OutputStream out = Files.newOutputStream(zip);
             ZipOutputStream zos = new ZipOutputStream(out)) {
            for (Map.Entry<String, String> entry : files.entrySet()) {
                zos.putNextEntry(new ZipEntry(entry.getKey()));
                zos.write(entry.getValue().getBytes(StandardCharsets.UTF_8));
                zos.closeEntry();
            }
        }
        return zip;
    }

    private static final StructType POSITION_SCHEMA = new StructType()
            .add("vehicleId", DataTypes.StringType)
            .add("tripId", DataTypes.StringType)
            .add("latitude", DataTypes.DoubleType)
            .add("longitude", DataTypes.DoubleType)
            .add("ts", DataTypes.TimestampType);

    private static Row position(String vehicleId, String tripId, double milliDegNorth, long epochMillis) {
        return RowFactory.create(vehicleId, tripId,
                BASE_LAT + milliDegNorth / 1000.0, BASE_LON, new Timestamp(epochMillis));
    }

    private Dataset<Row> projected(List<Row> rows) {
        return spark.createDataFrame(rows, POSITION_SCHEMA)
                .withColumn("proj", projectUdf.apply(
                        col("tripId"), col("latitude"), col("longitude")))
                .filter(col("proj").isNotNull())
                .select(col("vehicleId"), col("ts"),
                        col("proj.headwayGroup").as("headwayGroup"),
                        col("proj.distanceAlongRouteMetres").as("distanceMetres"),
                        col("proj.crossTrackMetres").as("crossTrackMetres"));
    }

    @Test
    @DisplayName("the projection UDF resolves a trip and returns distance along the route")
    void projectionUdfWorks() {
        List<Row> out = projected(List.of(position("bus-1", "T-OUT", 5.0, 1_000_000)))
                .collectAsList();

        assertThat(out).hasSize(1);
        Row row = out.getFirst();
        assertThat(row.<String>getAs("headwayGroup")).isEqualTo("15:0");
        // Five 0.001-degree steps north = 5 * 111.195 m.
        assertThat(row.<Double>getAs("distanceMetres")).isCloseTo(555.98, within(1.0));
        assertThat(row.<Double>getAs("crossTrackMetres")).isCloseTo(0.0, within(0.5));
    }

    @Test
    @DisplayName("direction from the static feed separates the two headway groups")
    void directionSeparatesGroups() {
        List<Row> out = projected(List.of(
                position("bus-out", "T-OUT", 3.0, 1_000_000),
                position("bus-in", "T-IN", 3.0, 1_000_000))).collectAsList();

        assertThat(out).extracting(r -> r.<String>getAs("headwayGroup"))
                .containsExactlyInAnyOrder("15:0", "15:1");
    }

    @Test
    @DisplayName("an unresolvable trip yields null and is filtered out, not a broken row")
    void unknownTripIsDropped() {
        List<Row> out = projected(List.of(
                position("bus-1", "T-OUT", 2.0, 1_000_000),
                position("ghost", "NOT-A-TRIP", 2.0, 1_000_000))).collectAsList();

        assertThat(out).hasSize(1);
        assertThat(out.getFirst().<String>getAs("vehicleId")).isEqualTo("bus-1");
    }

    /**
     * The full shape of the streaming aggregation, run as a batch: group, collect into an array of
     * structs, then hand that array to the gaps UDF. This is where the Scala {@code Seq} arrives.
     */
    @Test
    @DisplayName("the gaps UDF works on a real collect_list column")
    void gapsUdfWorksThroughSpark() {
        Dataset<Row> result = projected(List.of(
                        position("a", "T-OUT", 1.0, 1_000_000),
                        position("b", "T-OUT", 4.0, 1_000_000),
                        position("c", "T-OUT", 9.0, 1_000_000)))
                .groupBy(col("headwayGroup"))
                .agg(collect_list(struct(col("vehicleId"), col("distanceMetres"), col("ts")))
                        .as("sightings"))
                .withColumn("h", gapsUdf.apply(col("sightings"), col("headwayGroup"),
                        org.apache.spark.sql.functions.lit(Double.NaN),
                        org.apache.spark.sql.functions.lit(null)
                                .cast(DataTypes.TimestampType)));

        List<Row> out = result.collectAsList();
        assertThat(out).hasSize(1);

        Row h = out.getFirst().getAs("h");
        assertThat(h.<Integer>getAs("vehicleCount")).isEqualTo(3);

        List<Double> gaps = h.getList(h.fieldIndex("gapsMetres"));
        // 3 and 5 steps of 111.195 m.
        assertThat(gaps).hasSize(2);
        assertThat(gaps.get(0)).isCloseTo(333.6, within(2.0));
        assertThat(gaps.get(1)).isCloseTo(555.98, within(2.0));

        List<String> vehicles = h.getList(h.fieldIndex("orderedVehicles"));
        assertThat(vehicles).containsExactly("a", "b", "c");
    }

    /**
     * The same regression as {@code HeadwayGapsTest.oneBusIsNotAConvoy}, but verified through the
     * actual Spark path — because that is the path production takes, and a conversion bug between
     * Catalyst rows and the pure logic would reintroduce it without failing the pure test.
     */
    @Test
    @DisplayName("repeated sightings of one bus do not become a convoy in Spark either")
    void repeatedSightingsDoNotBecomeGaps() {
        List<Row> rows = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            rows.add(position("bus-1", "T-OUT", 1.0 + i, 1_000_000 + i * 15_000L));
        }

        Dataset<Row> result = projected(rows)
                .groupBy(col("headwayGroup"))
                .agg(collect_list(struct(col("vehicleId"), col("distanceMetres"), col("ts")))
                        .as("sightings"))
                .withColumn("h", gapsUdf.apply(col("sightings"), col("headwayGroup"),
                        org.apache.spark.sql.functions.lit(Double.NaN),
                        org.apache.spark.sql.functions.lit(null)
                                .cast(DataTypes.TimestampType)));

        Row h = result.collectAsList().getFirst().getAs("h");

        assertThat(h.<Integer>getAs("vehicleCount")).isEqualTo(1);
        assertThat(h.getList(h.fieldIndex("gapsMetres"))).isEmpty();

        // The newest reading, four steps north, must be the one kept.
        List<Double> distances = h.getList(h.fieldIndex("orderedDistancesMetres"));
        assertThat(distances).hasSize(1);
        assertThat(distances.getFirst()).isCloseTo(4 * 111.195, within(2.0));
    }

    @Test
    @DisplayName("the declared JSON schema parses what the ingest side actually writes")
    void schemaMatchesProducerOutput() {
        // Copied from a real message on the vehicle-positions topic.
        String json = "{\"vehicleId\":\"2322\",\"routeId\":\"15\",\"tripId\":\"10785433\","
                + "\"directionId\":5,\"latitude\":33.79291915893555,\"longitude\":-84.3209228515625,"
                + "\"bearingDegrees\":null,\"speedMetersPerSecond\":null,"
                + "\"timestamp\":\"2026-08-14T22:54:08Z\"}";

        Dataset<Row> parsed = spark
                .createDataset(List.of(json), org.apache.spark.sql.Encoders.STRING())
                .select(org.apache.spark.sql.functions.from_json(
                        col("value"), HeadwaySchema.VEHICLE_POSITION).as("p"))
                .select("p.*");

        Row row = parsed.collectAsList().getFirst();
        assertThat(row.<String>getAs("vehicleId")).isEqualTo("2322");
        assertThat(row.<String>getAs("tripId")).isEqualTo("10785433");
        assertThat(row.<Double>getAs("latitude")).isCloseTo(33.79291915893555, within(1e-9));
        assertThat(row.<Timestamp>getAs("timestamp").toInstant())
                .isEqualTo(Instant.parse("2026-08-14T22:54:08Z"));
        assertThat(row.<Double>getAs("speedMetersPerSecond")).isNull();
    }
}
