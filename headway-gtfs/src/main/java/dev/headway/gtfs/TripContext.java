package dev.headway.gtfs;

/**
 * Everything the scheduled feed knows about one realtime sighting, resolved in one lookup.
 *
 * <p>This is the object that turns a bare GPS ping into something a headway calculation can use:
 * which route it really is, which way it is going, and what path it follows.
 *
 * @param route the scheduled route, named and coloured
 * @param trip the specific scheduled run, carrying the <em>trustworthy</em> direction
 * @param shape the polyline the trip follows, which step 6 projects the GPS point onto
 */
public record TripContext(GtfsRoute route, GtfsTrip trip, RouteShape shape)
        implements java.io.Serializable {

    /**
     * The key that groups buses which can meaningfully bunch with each other.
     *
     * <p>Not the route alone. Two buses on route 15 heading in opposite directions are not
     * consecutive in any useful sense, and measuring the gap between them would report bunching
     * that is not happening. Grouping by route <em>and</em> direction is what makes the gap real.
     */
    public String headwayGroup() {
        return route.shortName() + ":" + trip.directionId();
    }
}
