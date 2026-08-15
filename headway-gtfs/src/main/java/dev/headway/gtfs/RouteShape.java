package dev.headway.gtfs;

/**
 * The physical path a route follows, as an ordered polyline.
 *
 * <h2>Why three primitive arrays instead of a List of point objects</h2>
 *
 * MARTA's {@code shapes.txt} holds 359,676 points across 215 shapes. Modelled as
 * {@code List<ShapePoint>} that is 359,676 heap objects, each with a header and each reached
 * through a pointer. Step 6 walks these arrays for every vehicle on every poll to find the nearest
 * segment — a tight numeric loop over the whole shape.
 *
 * <p>Three {@code double[]} arrays store the same data in about 8.6 MB of contiguous memory with
 * zero per-point objects. The loop then reads sequentially, which the CPU prefetcher handles well
 * and the JIT can vectorise. This is one of the few places where the unfashionable
 * data-oriented layout is clearly right: the access pattern is "scan everything, numerically".
 *
 * <p>The arrays are parallel — index {@code i} of each refers to the same point — and never
 * escape: {@link #latitudes()} and friends hand out copies, because a shared mutable array would
 * undo the immutability the rest of the system relies on.
 *
 * <h2>Units</h2>
 *
 * MARTA publishes {@code shape_dist_traveled} in <b>kilometres</b>. Verified by summing haversine
 * distance along shape 136092 and comparing: 10,815 m of haversine against a final
 * {@code shape_dist_traveled} of 10.8389, a ratio of 997.8 m per unit. Distances are converted to
 * <b>metres</b> at parse time so exactly one unit exists everywhere else in the codebase.
 */
public final class RouteShape implements java.io.Serializable {

    private static final long serialVersionUID = 1L;

    private final String shapeId;
    private final double[] lat;
    private final double[] lon;
    private final double[] cumulativeMetres;

    RouteShape(String shapeId, double[] lat, double[] lon, double[] cumulativeMetres) {
        if (lat.length != lon.length || lat.length != cumulativeMetres.length) {
            throw new IllegalArgumentException("parallel arrays must be the same length");
        }
        if (lat.length < 2) {
            throw new IllegalArgumentException(
                    "shape " + shapeId + " has " + lat.length + " points; a path needs at least 2");
        }
        this.shapeId = shapeId;
        this.lat = lat;
        this.lon = lon;
        this.cumulativeMetres = cumulativeMetres;
    }

    public String shapeId() {
        return shapeId;
    }

    public int pointCount() {
        return lat.length;
    }

    /** Total length of the path in metres. */
    public double lengthMetres() {
        return cumulativeMetres[cumulativeMetres.length - 1];
    }

    public double latitudeAt(int index) {
        return lat[index];
    }

    public double longitudeAt(int index) {
        return lon[index];
    }

    /** Distance from the start of the shape to point {@code index}, in metres. */
    public double distanceAt(int index) {
        return cumulativeMetres[index];
    }

    /** Defensive copies. Step 6 uses the indexed accessors above and copies nothing. */
    public double[] latitudes() {
        return lat.clone();
    }

    public double[] longitudes() {
        return lon.clone();
    }

    public double[] cumulativeMetres() {
        return cumulativeMetres.clone();
    }

    @Override
    public String toString() {
        return "RouteShape[%s, %d points, %.1f km]"
                .formatted(shapeId, pointCount(), lengthMetres() / 1000);
    }
}
