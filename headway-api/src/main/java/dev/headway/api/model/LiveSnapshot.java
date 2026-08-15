package dev.headway.api.model;

import dev.headway.common.VehiclePosition;
import java.time.Instant;
import java.util.List;

/**
 * The whole world as of one instant — one WebSocket frame.
 *
 * <h2>Snapshots, not deltas</h2>
 *
 * Sending "route 51 changed" would be smaller. It would also mean a browser that misses one frame
 * is subtly wrong until the next full refresh, that a client connecting mid-stream needs a separate
 * bootstrap path, and that reconnect-after-a-tunnel needs replay. All of that is real work to build
 * and more to debug.
 *
 * <p>A snapshot has none of those problems: every frame is self-contained, a dropped frame costs
 * one second of freshness, and connecting is the same code path as updating. It is affordable
 * because the state is small — a couple of hundred routes and a few hundred buses, a few tens of
 * kilobytes at one frame a second. Deltas would be the right answer at a hundred times this size,
 * and the wrong answer here.
 *
 * @param sequence increments per frame, so a client can tell how many it missed
 */
public record LiveSnapshot(
        Instant generatedAt,
        long sequence,
        List<RouteHeadway> routes,
        List<AlertEpisode> alerts,
        List<VehiclePosition> vehicles,
        ServiceStatus status) {
}
