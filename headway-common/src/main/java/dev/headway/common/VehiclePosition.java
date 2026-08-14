package dev.headway.common;

import java.time.Instant;
import java.util.Objects;
import java.util.OptionalDouble;

/**
 * One GPS ping from one bus, at one moment in time.
 *
 * <p>This is a Java {@code record}: an immutable data carrier. The compiler generates the
 * constructor, the getters ({@code vehicleId()}, not {@code getVehicleId()}),
 * {@code equals}/{@code hashCode}, and {@code toString} for us.
 *
 * <p>Immutability is a deliberate choice, not a style preference. Later in this project many
 * threads will read these objects concurrently out of a shared map. An object whose fields can
 * never change after construction is <em>safe to share between threads with no locking at all</em>.
 * That single decision removes an entire category of bug.
 */
public record VehiclePosition(
        String vehicleId,
        String routeId,
        String tripId,
        Integer directionId,
        double latitude,
        double longitude,
        Float bearingDegrees,
        Float speedMetersPerSecond,
        Instant timestamp) {

    /**
     * The "compact constructor". Runs before the fields are assigned, so it is where we validate.
     *
     * <p>Rejecting bad data at the boundary means every {@code VehiclePosition} that exists
     * anywhere in the program is known-good. Nothing downstream has to re-check.
     */
    public VehiclePosition {
        Objects.requireNonNull(vehicleId, "vehicleId");
        Objects.requireNonNull(routeId, "routeId");
        Objects.requireNonNull(timestamp, "timestamp");

        if (latitude < -90 || latitude > 90) {
            throw new IllegalArgumentException("latitude out of range: " + latitude);
        }
        if (longitude < -180 || longitude > 180) {
            throw new IllegalArgumentException("longitude out of range: " + longitude);
        }
    }

    /** {@code speedMetersPerSecond} is optional in the feed; this avoids null-checks at call sites. */
    public OptionalDouble speed() {
        return speedMetersPerSecond == null
                ? OptionalDouble.empty()
                : OptionalDouble.of(speedMetersPerSecond);
    }

    /**
     * True if {@code other} is a strictly newer reading for the same vehicle.
     *
     * <p>Feeds re-send the same ping, and pings can arrive out of order. Every update we ever
     * apply will be gated on this check, which makes ingestion <em>idempotent</em>: replaying the
     * same message a hundred times leaves the system in exactly the state one delivery would.
     */
    public boolean isSupersededBy(VehiclePosition other) {
        return other != null
                && other.vehicleId.equals(this.vehicleId)
                && other.timestamp.isAfter(this.timestamp);
    }
}
