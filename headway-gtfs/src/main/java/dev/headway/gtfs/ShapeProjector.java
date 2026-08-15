package dev.headway.gtfs;

/**
 * Snaps a GPS point onto a route's path and reports how far along it is.
 *
 * <p>This is the step that makes headway computable at all. Two buses at
 * {@code (33.75, -84.39)} and {@code (33.77, -84.41)} are 2.7 km apart as the crow flies, but that
 * number is meaningless — buses follow streets, not straight lines, and the route between them may
 * be 2.8 km or 9 km depending on how it winds. Projecting both onto the shape converts two
 * coordinates into two positions on a one-dimensional line, and on a line "the gap between them"
 * is simply a subtraction.
 *
 * <h2>The projection</h2>
 *
 * Latitude and longitude are angles, and planar geometry on angles is wrong: at Atlanta's latitude
 * one degree of longitude is about 92.6 km while one degree of latitude is about 111.3 km. Treating
 * them as equivalent would stretch every east-west distance by 20% and pick the wrong segment near
 * diagonal corners.
 *
 * <p>So each segment is converted into a local east-north plane in metres, centred on the query
 * point itself:
 *
 * <pre>{@code
 * x = (lon - queryLon) * metresPerDegreeLat * cos(queryLat)
 * y = (lat - queryLat) * metresPerDegreeLat
 * }</pre>
 *
 * This is the equirectangular approximation. Over the tens of kilometres a bus route spans it is
 * accurate to well under a metre, and it costs two multiplications instead of the trigonometry a
 * full geodesic calculation needs — which matters when this runs for every vehicle on every poll.
 * Centring on the query point also puts it at the origin, so the point-to-segment maths below
 * simplifies to distances from zero.
 *
 * <h2>Distance along the route</h2>
 *
 * Once the nearest segment and the fraction {@code t} along it are known, the answer interpolates
 * the feed's own {@code shape_dist_traveled} values rather than summing computed segment lengths.
 * The agency's numbers follow the real road geometry, so they are the better authority — and using
 * them means our projection error never accumulates along the route.
 */
public final class ShapeProjector {

    /** Metres per degree of latitude: {@code 2 * PI * R / 360} for the IUGG mean radius. */
    private static final double METRES_PER_DEGREE_LAT = 111_195.08;

    private ShapeProjector() {}

    /**
     * Snaps a point onto the whole shape, scanning every segment.
     *
     * <p><b>Caveat for looping routes.</b> This takes the globally nearest segment. If a route
     * passes the same place twice — a loop, or an out-and-back along one street — two segments are
     * near-equally close, and tiny GPS jitter can flip the answer between them. The reported
     * distance then jumps by kilometres and the bus appears to teleport. Use
     * {@link #projectNear} once a previous position is known; it is immune to this.
     */
    public static ShapeProjection project(RouteShape shape, double latitude, double longitude) {
        return scan(shape, latitude, longitude, 0, shape.pointCount() - 2);
    }

    /**
     * Snaps a point, considering only the part of the route near a previous known position.
     *
     * <p>This is the fix for the loop ambiguity above, and it is also faster. A bus that was at
     * 8,000 m along the route fifteen seconds ago is still within a few hundred metres of that, so
     * searching {@code [hint - window, hint + window]} both removes the far-away duplicate segment
     * from consideration and scans a fraction of the polyline.
     *
     * <p>Falls back to a full scan when the window contains no segments — which happens on the
     * first sighting of a vehicle, or when a bus really has jumped (deadheading, or a long GPS
     * outage). Better a possibly-ambiguous answer than none.
     *
     * @param hintMetres the vehicle's previous distance along this shape
     * @param windowMetres how far it could plausibly have moved since, in either direction
     */
    public static ShapeProjection projectNear(RouteShape shape, double latitude, double longitude,
                                              double hintMetres, double windowMetres) {
        int n = shape.pointCount();
        double low = hintMetres - windowMetres;
        double high = hintMetres + windowMetres;

        // cumulativeMetres is sorted, so binary search finds the window bounds in log time.
        int first = firstSegmentAtOrAfter(shape, low);
        int last = lastSegmentStartingBefore(shape, high);

        if (first > last || first > n - 2) {
            return project(shape, latitude, longitude);
        }
        return scan(shape, latitude, longitude, first, Math.min(last, n - 2));
    }

    /** Scans segments {@code [fromSegment, toSegment]} inclusive and returns the nearest. */
    private static ShapeProjection scan(RouteShape shape, double latitude, double longitude,
                                        int fromSegment, int toSegment) {
        if (shape.pointCount() < 2) {
            throw new IllegalArgumentException("shape " + shape.shapeId() + " has no segments");
        }

        // One cosine for the whole scan. Recomputing it per segment would dominate the loop, and
        // over a single route the latitude barely changes.
        final double metresPerDegreeLon =
                METRES_PER_DEGREE_LAT * Math.cos(Math.toRadians(latitude));

        double bestDistanceSq = Double.POSITIVE_INFINITY;
        int bestSegment = fromSegment;
        double bestT = 0;

        // Working in metres relative to the query point puts the point at the origin, so the
        // distance from the point to a candidate is just the length of the candidate vector.
        double ax = (shape.longitudeAt(fromSegment) - longitude) * metresPerDegreeLon;
        double ay = (shape.latitudeAt(fromSegment) - latitude) * METRES_PER_DEGREE_LAT;

        for (int i = fromSegment; i <= toSegment; i++) {
            double bx = (shape.longitudeAt(i + 1) - longitude) * metresPerDegreeLon;
            double by = (shape.latitudeAt(i + 1) - latitude) * METRES_PER_DEGREE_LAT;

            double dx = bx - ax;
            double dy = by - ay;
            double lengthSq = dx * dx + dy * dy;

            // Fraction along AB of the foot of the perpendicular from the origin.
            // Normally t = ((P-A).(B-A)) / |B-A|^2; with P at the origin that is (-A).(B-A).
            double t = lengthSq == 0 ? 0 : -(ax * dx + ay * dy) / lengthSq;

            // Clamping is what makes this a segment rather than an infinite line. Without it a
            // bus near a bend would project onto an imaginary extension of the road.
            if (t < 0) {
                t = 0;
            } else if (t > 1) {
                t = 1;
            }

            double cx = ax + t * dx;
            double cy = ay + t * dy;
            double distanceSq = cx * cx + cy * cy;

            if (distanceSq < bestDistanceSq) {
                bestDistanceSq = distanceSq;
                bestSegment = i;
                bestT = t;
            }

            // The end of this segment is the start of the next; no need to recompute it.
            ax = bx;
            ay = by;
        }

        double startMetres = shape.distanceAt(bestSegment);
        double endMetres = shape.distanceAt(bestSegment + 1);
        double alongMetres = startMetres + bestT * (endMetres - startMetres);

        double lat0 = shape.latitudeAt(bestSegment);
        double lon0 = shape.longitudeAt(bestSegment);
        double snappedLat = lat0 + bestT * (shape.latitudeAt(bestSegment + 1) - lat0);
        double snappedLon = lon0 + bestT * (shape.longitudeAt(bestSegment + 1) - lon0);

        return new ShapeProjection(
                alongMetres, Math.sqrt(bestDistanceSq), bestSegment, snappedLat, snappedLon);
    }

    /** Index of the first segment whose far end reaches {@code metres}. */
    private static int firstSegmentAtOrAfter(RouteShape shape, double metres) {
        int low = 0;
        int high = shape.pointCount() - 1;
        while (low < high) {
            int mid = (low + high) >>> 1;
            if (shape.distanceAt(mid + 1) < metres) {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        return low;
    }

    /** Index of the last segment that begins before {@code metres}. */
    private static int lastSegmentStartingBefore(RouteShape shape, double metres) {
        int low = 0;
        int high = shape.pointCount() - 2;
        while (low < high) {
            int mid = (low + high + 1) >>> 1;
            if (shape.distanceAt(mid) <= metres) {
                low = mid;
            } else {
                high = mid - 1;
            }
        }
        return low;
    }
}
