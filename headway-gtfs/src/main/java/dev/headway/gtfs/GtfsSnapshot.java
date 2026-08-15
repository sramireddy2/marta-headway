package dev.headway.gtfs;

import com.google.common.collect.ImmutableMap;
import java.time.Instant;
import java.util.Optional;

/**
 * One parsed version of the scheduled feed, frozen and safe to share across threads.
 *
 * <p>Everything here is immutable, so a snapshot can be handed to any number of readers with no
 * locking. When a newer feed is published, {@link GtfsStaticRepository} builds an entirely new
 * snapshot and swaps the reference; nobody mutates anything in place.
 *
 * <h2>The two joins the realtime side needs</h2>
 *
 * <ul>
 *   <li>{@link #routeForRealtimeId} — realtime {@code route_id} is the static
 *       {@code route_short_name}, not the static {@code route_id}.
 *   <li>{@link #trip} — realtime {@code trip_id} <em>does</em> match the static {@code trip_id}
 *       directly, and is the only trustworthy source of travel direction.
 * </ul>
 */
public final class GtfsSnapshot {

    private final ImmutableMap<String, GtfsRoute> routesByStaticId;
    private final ImmutableMap<String, GtfsRoute> routesByShortName;
    private final ImmutableMap<String, GtfsTrip> tripsById;
    private final ImmutableMap<String, RouteShape> shapesById;
    private final Instant loadedAt;
    private final Instant feedLastModified;

    GtfsSnapshot(ImmutableMap<String, GtfsRoute> routesByStaticId,
                 ImmutableMap<String, GtfsRoute> routesByShortName,
                 ImmutableMap<String, GtfsTrip> tripsById,
                 ImmutableMap<String, RouteShape> shapesById,
                 Instant loadedAt,
                 Instant feedLastModified) {
        this.routesByStaticId = routesByStaticId;
        this.routesByShortName = routesByShortName;
        this.tripsById = tripsById;
        this.shapesById = shapesById;
        this.loadedAt = loadedAt;
        this.feedLastModified = feedLastModified;
    }

    /**
     * Looks up a route by the id the <b>realtime</b> feed uses.
     *
     * <p>Named for what it does rather than what it looks up, because
     * {@code routeById(realtimeRouteId)} is the mistake this whole class is trying to prevent.
     */
    public Optional<GtfsRoute> routeForRealtimeId(String realtimeRouteId) {
        return Optional.ofNullable(routesByShortName.get(realtimeRouteId));
    }

    /** Looks up a route by the static feed's own {@code route_id}. Rarely what you want. */
    public Optional<GtfsRoute> routeByStaticId(String staticRouteId) {
        return Optional.ofNullable(routesByStaticId.get(staticRouteId));
    }

    public Optional<GtfsTrip> trip(String tripId) {
        return tripId == null ? Optional.empty() : Optional.ofNullable(tripsById.get(tripId));
    }

    public Optional<RouteShape> shape(String shapeId) {
        return shapeId == null ? Optional.empty() : Optional.ofNullable(shapesById.get(shapeId));
    }

    /**
     * The path a given trip physically follows.
     *
     * <p>Note this is per <em>trip</em>, not per route: one route usually has at least two shapes,
     * one per direction, and often more for short-turn or detour variants.
     */
    public Optional<RouteShape> shapeForTrip(String tripId) {
        return trip(tripId).flatMap(t -> shape(t.shapeId()));
    }

    /**
     * Resolves a realtime sighting into everything the scheduled feed knows about it.
     *
     * @param realtimeRouteId the realtime feed's {@code route_id} (a static short name)
     * @param tripId the realtime feed's {@code trip_id}
     */
    public Optional<TripContext> resolve(String realtimeRouteId, String tripId) {
        Optional<GtfsTrip> trip = trip(tripId);
        if (trip.isEmpty()) {
            return Optional.empty();
        }
        GtfsTrip t = trip.get();
        RouteShape shape = shapesById.get(t.shapeId());
        if (shape == null) {
            return Optional.empty();
        }
        GtfsRoute route = routesByStaticId.get(t.routeId());
        if (route == null) {
            route = routesByShortName.get(realtimeRouteId);
        }
        return route == null ? Optional.empty() : Optional.of(new TripContext(route, t, shape));
    }

    public ImmutableMap<String, GtfsRoute> routesByShortName() {
        return routesByShortName;
    }

    public ImmutableMap<String, GtfsTrip> tripsById() {
        return tripsById;
    }

    public ImmutableMap<String, RouteShape> shapesById() {
        return shapesById;
    }

    public int routeCount() {
        return routesByStaticId.size();
    }

    public int tripCount() {
        return tripsById.size();
    }

    public int shapeCount() {
        return shapesById.size();
    }

    /** When this snapshot was parsed. */
    public Instant loadedAt() {
        return loadedAt;
    }

    /** {@code Last-Modified} of the zip it came from, or {@link Instant#EPOCH} if unknown. */
    public Instant feedLastModified() {
        return feedLastModified;
    }

    @Override
    public String toString() {
        return "GtfsSnapshot[%d routes, %d trips, %d shapes, feed published %s]"
                .formatted(routeCount(), tripCount(), shapeCount(), feedLastModified);
    }
}
