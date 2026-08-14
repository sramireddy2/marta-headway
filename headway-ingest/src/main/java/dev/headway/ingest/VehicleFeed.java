package dev.headway.ingest;

import dev.headway.common.VehiclePosition;
import java.io.IOException;
import java.util.List;

/**
 * Anything that can produce a batch of vehicle positions.
 *
 * <p>{@link FeedPoller} depends on this interface rather than on {@link GtfsRealtimeClient}
 * directly. That single indirection buys three things:
 *
 * <ul>
 *   <li>tests can supply a fake feed that returns canned data, or one that always fails, without
 *       any mocking framework and without touching the network;
 *   <li>step 5 can add a second implementation for the trip-updates feed;
 *   <li>a replay-from-disk implementation becomes trivial when you want to demo the app offline.
 * </ul>
 *
 * <p>Keeping the interface this small is the point. An interface with one method is easy to
 * implement by hand; an interface with twelve forces you to reach for Mockito.
 */
@FunctionalInterface
public interface VehicleFeed {

    /** @return the current batch of positions; may be empty, never null */
    List<VehiclePosition> fetchVehiclePositions() throws IOException, InterruptedException;
}
