package dev.headway.gtfs;

import com.google.common.collect.ImmutableMap;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Turns {@code google_transit.zip} into a {@link GtfsSnapshot}.
 *
 * <h2>Reading three files out of a 147 MB archive</h2>
 *
 * Uncompressed, MARTA's feed is about 147 MB — and 126 MB of that is {@code stop_times.txt}, which
 * we do not need until scheduled headways in step 8. Using {@link java.util.zip.ZipInputStream}
 * would force us to decompress every entry in order just to reach the ones we want.
 * {@link ZipFile} reads the archive's central directory and opens individual entries, so
 * {@code stop_times.txt} is never decompressed at all. Same file, a fraction of the work.
 *
 * <h2>Why a CSV library</h2>
 *
 * {@code line.split(",")} looks sufficient until a field contains a comma —
 * {@code "Donald Lee Hollowell/Ponce de Leon, NW"} — at which point every column after it shifts
 * by one and the failure is silent. Commons CSV handles quoting, and reading by header name
 * instead of column index means a reordered column in a future feed revision cannot break us.
 */
public final class GtfsStaticLoader {

    private static final Logger log = LoggerFactory.getLogger(GtfsStaticLoader.class);

    /** MARTA publishes {@code shape_dist_traveled} in kilometres; we normalise to metres. */
    private static final double SHAPE_DIST_UNITS_TO_METRES = 1000.0;

    private static final CSVFormat FORMAT = CSVFormat.DEFAULT.builder()
            .setHeader()
            .setSkipHeaderRecord(true)
            .setIgnoreSurroundingSpaces(true)
            .setIgnoreEmptyLines(true)
            .get();

    public GtfsSnapshot load(Path zipPath, Instant feedLastModified) throws IOException {
        long start = System.nanoTime();

        try (ZipFile zip = new ZipFile(zipPath.toFile())) {
            Map<String, GtfsRoute> byStaticId = new HashMap<>();
            Map<String, GtfsRoute> byShortName = new HashMap<>();
            readRoutes(zip, byStaticId, byShortName);

            Map<String, GtfsTrip> trips = readTrips(zip);
            Map<String, RouteShape> shapes = readShapes(zip);

            GtfsSnapshot snapshot = new GtfsSnapshot(
                    ImmutableMap.copyOf(byStaticId),
                    ImmutableMap.copyOf(byShortName),
                    ImmutableMap.copyOf(trips),
                    ImmutableMap.copyOf(shapes),
                    Instant.now(),
                    feedLastModified);

            log.info("Loaded static GTFS in {}ms: {}",
                    (System.nanoTime() - start) / 1_000_000, snapshot);
            return snapshot;
        }
    }

    private void readRoutes(ZipFile zip, Map<String, GtfsRoute> byStaticId,
                            Map<String, GtfsRoute> byShortName) throws IOException {
        try (CSVParser parser = open(zip, "routes.txt")) {
            for (CSVRecord row : parser) {
                String routeId = row.get("route_id");
                String shortName = get(row, "route_short_name", "");
                GtfsRoute route = new GtfsRoute(
                        routeId,
                        shortName,
                        get(row, "route_long_name", ""),
                        parseInt(get(row, "route_type", "3"), 3),
                        get(row, "route_color", ""));

                byStaticId.put(routeId, route);

                if (!shortName.isEmpty()) {
                    // A duplicate short name would mean the realtime join is ambiguous, which
                    // would silently attach vehicles to the wrong route. Better to know.
                    GtfsRoute clash = byShortName.putIfAbsent(shortName, route);
                    if (clash != null) {
                        log.warn("route_short_name '{}' is used by both route_id {} and {}; "
                                        + "realtime lookups for it are ambiguous",
                                shortName, clash.routeId(), routeId);
                    }
                }
            }
        }
    }

    private Map<String, GtfsTrip> readTrips(ZipFile zip) throws IOException {
        Map<String, GtfsTrip> trips = new HashMap<>(64_000);
        int skipped = 0;

        try (CSVParser parser = open(zip, "trips.txt")) {
            for (CSVRecord row : parser) {
                String tripId = row.get("trip_id");
                String shapeId = get(row, "shape_id", "");
                String rawDirection = get(row, "direction_id", "");

                // A trip with no shape cannot be projected onto a path, and a trip with no
                // direction cannot be grouped for headway. Either way it is unusable, so drop it
                // here rather than have every downstream stage re-check.
                if (shapeId.isEmpty() || rawDirection.isEmpty()) {
                    skipped++;
                    continue;
                }
                int direction = parseInt(rawDirection, -1);
                if (direction != 0 && direction != 1) {
                    skipped++;
                    continue;
                }

                trips.put(tripId, new GtfsTrip(
                        tripId, row.get("route_id"), direction, shapeId,
                        get(row, "trip_headsign", "")));
            }
        }
        if (skipped > 0) {
            log.warn("Skipped {} trips with no shape_id or an invalid direction_id", skipped);
        }
        return trips;
    }

    /**
     * Reads {@code shapes.txt} into per-shape primitive arrays.
     *
     * <p>Points are accumulated into growable lists first because the row count per shape is not
     * known until the file has been read, then copied into exactly-sized {@code double[]} arrays.
     * The lists are discarded immediately; only the compact arrays survive.
     *
     * <p>The file is expected to be grouped by {@code shape_id} and ordered by
     * {@code shape_pt_sequence} — MARTA's is, verified across all 359,676 points. It is still
     * checked rather than assumed, because a feed revision that broke the ordering would otherwise
     * produce a shape that zig-zags, and every distance computed from it would be wrong with no
     * error anywhere.
     */
    private Map<String, RouteShape> readShapes(ZipFile zip) throws IOException {
        Map<String, List<double[]>> raw = new HashMap<>();
        Map<String, Integer> lastSequence = new HashMap<>();
        int outOfOrder = 0;

        try (CSVParser parser = open(zip, "shapes.txt")) {
            for (CSVRecord row : parser) {
                String shapeId = row.get("shape_id");
                int sequence = parseInt(row.get("shape_pt_sequence"), -1);

                Integer previous = lastSequence.put(shapeId, sequence);
                if (previous != null && sequence <= previous) {
                    outOfOrder++;
                }

                raw.computeIfAbsent(shapeId, id -> new ArrayList<>(512)).add(new double[] {
                        Double.parseDouble(row.get("shape_pt_lat")),
                        Double.parseDouble(row.get("shape_pt_lon")),
                        parseDouble(get(row, "shape_dist_traveled", ""), Double.NaN)
                                * SHAPE_DIST_UNITS_TO_METRES
                });
            }
        }

        if (outOfOrder > 0) {
            log.warn("{} shape points arrived out of sequence; distances may be unreliable",
                    outOfOrder);
        }

        Map<String, RouteShape> shapes = new HashMap<>(raw.size() * 2);
        int computed = 0;
        for (Map.Entry<String, List<double[]>> entry : raw.entrySet()) {
            List<double[]> points = entry.getValue();
            if (points.size() < 2) {
                continue; // not a path
            }
            int n = points.size();
            double[] lat = new double[n];
            double[] lon = new double[n];
            double[] dist = new double[n];

            boolean distancesUsable = true;
            for (int i = 0; i < n; i++) {
                double[] p = points.get(i);
                lat[i] = p[0];
                lon[i] = p[1];
                dist[i] = p[2];
                if (Double.isNaN(dist[i]) || (i > 0 && dist[i] < dist[i - 1])) {
                    distancesUsable = false;
                }
            }

            // shape_dist_traveled is optional in GTFS and not every agency fills it in. When it is
            // missing or non-monotonic we compute cumulative distance ourselves, so this module
            // works against feeds other than MARTA's.
            if (!distancesUsable) {
                computeCumulativeDistances(lat, lon, dist);
                computed++;
            }

            shapes.put(entry.getKey(), new RouteShape(entry.getKey(), lat, lon, dist));
        }

        if (computed > 0) {
            log.info("Computed cumulative distance for {} of {} shapes "
                    + "(shape_dist_traveled missing or non-monotonic)", computed, shapes.size());
        }
        return shapes;
    }

    /** Fallback when the feed does not supply usable distances: haversine along the polyline. */
    private static void computeCumulativeDistances(double[] lat, double[] lon, double[] out) {
        out[0] = 0;
        for (int i = 1; i < lat.length; i++) {
            out[i] = out[i - 1] + haversineMetres(lat[i - 1], lon[i - 1], lat[i], lon[i]);
        }
    }

    static double haversineMetres(double lat1, double lon1, double lat2, double lon2) {
        final double earthRadiusMetres = 6_371_008.8; // IUGG mean radius
        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);
        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                * Math.sin(dLon / 2) * Math.sin(dLon / 2);
        return earthRadiusMetres * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
    }

    private static CSVParser open(ZipFile zip, String entryName) throws IOException {
        ZipEntry entry = zip.getEntry(entryName);
        if (entry == null) {
            throw new IOException("'" + entryName + "' is missing from the GTFS archive");
        }
        InputStream in = zip.getInputStream(entry);
        // A BOM at the start of the file would become part of the first header name, so the very
        // first column lookup would fail with a baffling "Mapping for route_id not found".
        Reader reader = new BufferedReader(
                new InputStreamReader(new BomStrippingInputStream(in), StandardCharsets.UTF_8),
                1 << 16);
        return CSVParser.builder().setReader(reader).setFormat(FORMAT).get();
    }

    private static String get(CSVRecord row, String column, String fallback) {
        if (!row.isMapped(column)) {
            return fallback;
        }
        String value = row.get(column);
        return value == null ? fallback : value.trim();
    }

    private static int parseInt(String value, int fallback) {
        try {
            return Integer.parseInt(value.trim());
        } catch (RuntimeException e) {
            return fallback;
        }
    }

    private static double parseDouble(String value, double fallback) {
        if (value == null || value.isBlank()) {
            return fallback;
        }
        try {
            return Double.parseDouble(value.trim());
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    /** Swallows a UTF-8 byte-order mark if the file starts with one. */
    private static final class BomStrippingInputStream extends InputStream {
        private final InputStream delegate;
        private boolean checked;

        BomStrippingInputStream(InputStream delegate) {
            this.delegate = delegate.markSupported()
                    ? delegate
                    : new java.io.BufferedInputStream(delegate);
        }

        @Override
        public int read() throws IOException {
            skipBomOnce();
            return delegate.read();
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            skipBomOnce();
            return delegate.read(b, off, len);
        }

        private void skipBomOnce() throws IOException {
            if (checked) {
                return;
            }
            checked = true;
            delegate.mark(3);
            byte[] first = new byte[3];
            int read = delegate.read(first, 0, 3);
            boolean isBom = read == 3
                    && (first[0] & 0xFF) == 0xEF
                    && (first[1] & 0xFF) == 0xBB
                    && (first[2] & 0xFF) == 0xBF;
            if (!isBom) {
                delegate.reset();
            }
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }
}
