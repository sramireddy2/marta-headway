package dev.headway.api.state;

import dev.headway.api.ApiProperties;
import dev.headway.api.model.AlertEpisode;
import java.time.Instant;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Forgets things that are no longer true.
 *
 * <p>Everything this service holds is a cache of a stream, and the stream never says "route 51 has
 * stopped for the night" — it just stops mentioning route 51. Absence of evidence is the only
 * signal available, so something has to act on it. Without this class the dashboard accumulates
 * every route and bus seen since startup and displays hours-old measurements as current, which is
 * strictly worse than displaying nothing.
 *
 * <p>The three timeouts are deliberately different, because they answer different questions.
 * A bus is stale after five minutes because that is roughly when a position stops being useful on a
 * map. A route lingers for ten because its timestamps are <em>event</em> time and already trail the
 * clock by the stream job's watermark plus its batch interval. An alert episode is presumed over
 * after three minutes of silence, the shortest of the three, because a stale alert is the most
 * actively misleading of the three things to leave on screen.
 */
@Component
public final class Housekeeping {

    private static final Logger log = LoggerFactory.getLogger(Housekeeping.class);

    private final ApiProperties properties;
    private final LiveHeadwayState headways;
    private final VehicleState vehicles;
    private final AlertTracker alerts;

    public Housekeeping(ApiProperties properties, LiveHeadwayState headways, VehicleState vehicles,
            AlertTracker alerts) {
        this.properties = properties;
        this.headways = headways;
        this.vehicles = vehicles;
        this.alerts = alerts;
    }

    /**
     * {@code fixedDelay}, not {@code fixedRate}. If a sweep ever ran longer than the interval,
     * fixedRate would schedule the next one immediately and keep doing so, and the scheduler would
     * spend all its time sweeping. fixedDelay always leaves the gap.
     */
    @Scheduled(fixedDelayString = "${headway.sweep-interval-millis:15000}")
    public void sweep() {
        Instant now = Instant.now();

        int routesDropped = headways.evictStale(now, properties.routeTtl());
        int vehiclesDropped = vehicles.evictStale(now, properties.vehicleTtl());
        List<AlertEpisode> closed = alerts.sweep(now, properties.alertLinger());

        for (AlertEpisode episode : closed) {
            log.info("CLEARED (timed out) {} on {} after {} - peaked at {}",
                    episode.worstStatus(), episode.routeLongName(), episode.duration(),
                    episode.worstRatio());
        }
        if (routesDropped > 0 || vehiclesDropped > 0) {
            log.debug("Swept {} routes and {} vehicles", routesDropped, vehiclesDropped);
        }
    }
}
