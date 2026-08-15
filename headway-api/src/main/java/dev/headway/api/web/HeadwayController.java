package dev.headway.api.web;

import dev.headway.api.model.AlertEpisode;
import dev.headway.api.model.LiveSnapshot;
import dev.headway.api.model.RouteHeadway;
import dev.headway.api.model.ServiceStatus;
import dev.headway.api.state.AlertTracker;
import dev.headway.api.state.LiveHeadwayState;
import dev.headway.api.state.VehicleState;
import dev.headway.common.VehiclePosition;
import java.util.Collection;
import java.util.List;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * The REST half of the API. The WebSocket pushes the same data; this is for anything that wants to
 * ask rather than listen — {@code curl}, a health check, a chart that loads once.
 *
 * <p>Every method here is a read of an in-memory map. No blocking I/O, no database, so Tomcat's
 * default thread pool is nowhere near being the bottleneck and none of this needs to be async.
 * That is a consequence of the design rather than luck: the Kafka consumers do the work off the
 * request path and leave behind a structure that is already in the shape the answer needs.
 */
@RestController
@RequestMapping("/api")
public class HeadwayController {

    private final LiveHeadwayState headways;
    private final VehicleState vehicles;
    private final AlertTracker alerts;
    private final SnapshotBroadcaster snapshots;

    public HeadwayController(LiveHeadwayState headways, VehicleState vehicles, AlertTracker alerts,
            SnapshotBroadcaster snapshots) {
        this.headways = headways;
        this.vehicles = vehicles;
        this.alerts = alerts;
        this.snapshots = snapshots;
    }

    /** Every route-direction group currently being measured, worst first. */
    @GetMapping("/routes")
    public List<RouteHeadway> routes(
            @RequestParam(name = "alertingOnly", defaultValue = "false") boolean alertingOnly) {
        List<RouteHeadway> all = headways.current();
        return alertingOnly ? all.stream().filter(RouteHeadway::isAlertable).toList() : all;
    }

    /**
     * One group, identified as {@code routeShortName:directionId} — for example {@code 10:1}.
     *
     * <p>The colon is a legal path character, but Spring truncates a path variable at the last dot
     * by default (a leftover from URLs like {@code /file.json}), so a route named {@code 5.5} would
     * otherwise arrive as {@code 5}. The regex makes the variable greedy to the end of the segment.
     */
    @GetMapping("/routes/{group:.+}")
    public ResponseEntity<RouteHeadway> route(@PathVariable(name = "group") String group) {
        return headways.group(group)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    /**
     * Alerts as <em>episodes</em>, not as window measurements.
     *
     * <p>This is the endpoint that fixes step 8's duplicate-alert problem. The {@code
     * headway-alerts} topic contains one record per window per alerting route, so a three-minute
     * bunching event appears in it six or more times. Here it is one row, with a start time, a
     * duration, and {@code windowsObserved} recording how many measurements agree.
     */
    @GetMapping("/alerts")
    public List<AlertEpisode> alerts() {
        return alerts.openEpisodes();
    }

    /** Episodes that have finished, newest first. */
    @GetMapping("/alerts/history")
    public List<AlertEpisode> alertHistory(
            @RequestParam(name = "limit", defaultValue = "50") int limit) {
        return alerts.recentHistory(Math.clamp(limit, 1, 500));
    }

    /** Last known position of every bus. This is what the map in step 10 draws. */
    @GetMapping("/vehicles")
    public Collection<VehiclePosition> vehicles(
            @RequestParam(name = "routeId", required = false) String routeId) {
        return routeId == null ? vehicles.all() : vehicles.onRoute(routeId);
    }

    /** Exactly what a WebSocket client receives, for anything that would rather poll. */
    @GetMapping("/snapshot")
    public LiveSnapshot snapshot() {
        return snapshots.snapshot();
    }

    /** Counters that distinguish "nothing is wrong" from "nothing is arriving". */
    @GetMapping("/status")
    public ServiceStatus status() {
        return snapshots.status();
    }
}
