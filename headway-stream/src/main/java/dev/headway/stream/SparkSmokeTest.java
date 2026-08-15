package dev.headway.stream;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Throwaway check that Spark starts at all on this machine before any real job is written.
 *
 * <p>Spark on Windows has two well-known friction points worth discovering now rather than in the
 * middle of debugging streaming logic: it expects a Hadoop {@code winutils.exe} for some
 * filesystem operations, and on Java 17+ it needs {@code --add-opens} flags because it reflects
 * into {@code java.base} internals that the module system closed off.
 */
public final class SparkSmokeTest {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("headway-smoke")
                .master("local[2]")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.shuffle.partitions", "4")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        System.out.println("Spark version : " + spark.version());
        System.out.println("Java version  : " + System.getProperty("java.version"));

        Dataset<Row> numbers = spark.range(1, 11).toDF("n");
        long count = numbers.count();
        long sum = numbers.selectExpr("sum(n) as total").first().getLong(0);

        System.out.println("count = " + count + " (expected 10)");
        System.out.println("sum   = " + sum + " (expected 55)");
        System.out.println(count == 10 && sum == 55 ? "SMOKE TEST PASSED" : "SMOKE TEST FAILED");

        spark.stop();
    }

    private SparkSmokeTest() {}
}
