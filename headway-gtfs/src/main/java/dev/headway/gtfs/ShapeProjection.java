package dev.headway.gtfs;

/**
 * Where a GPS point lands once it has been snapped onto a route's path.
 *
 * @param distanceAlongRouteMetres how far along the shape the bus is, measured from the start.
 *     <b>This is the number the entire headway calculation rests on.</b> The gap between two buses
 *     is the difference between their two values.
 * @param crossTrackMetres how far the raw GPS point was from the path. Small values are ordinary
 *     GPS scatter; large ones mean the bus is off route, the shape is wrong, or the fix is bad.
 *     Callers should treat this as a confidence signal and discard implausible projections.
 * @param segmentIndex the polyline segment the point snapped to, running from
 *     {@code segmentIndex} to {@code segmentIndex + 1}
 * @param snappedLatitude latitude of the snapped point, for drawing on a map
 * @param snappedLongitude longitude of the snapped point
 */
public record ShapeProjection(
        double distanceAlongRouteMetres,
        double crossTrackMetres,
        int segmentIndex,
        double snappedLatitude,
        double snappedLongitude) {

    /**
     * Is this projection trustworthy enough to compute a headway from?
     *
     * @param maxCrossTrackMetres how far off the path is still believable
     */
    public boolean isOnRoute(double maxCrossTrackMetres) {
        return crossTrackMetres <= maxCrossTrackMetres;
    }

    @Override
    public String toString() {
        return "ShapeProjection[%.1f m along, %.1f m off route, segment %d]"
                .formatted(distanceAlongRouteMetres, crossTrackMetres, segmentIndex);
    }
}
