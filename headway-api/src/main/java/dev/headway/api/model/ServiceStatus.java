package dev.headway.api.model;

import java.time.Instant;
import java.util.List;

/**
 * What the service itself is doing — the endpoint to look at when the dashboard is empty.
 *
 * <p>An empty dashboard has several very different causes: Kafka unreachable, Kafka fine but the
 * stream job not running, the stream job running but writing a shape this API cannot parse, or a
 * genuinely quiet network at 3am. They look identical from the outside and are distinguished by the
 * counters below.
 *
 * @param headwayRecords records read from the headway topic; zero means nothing is arriving
 * @param malformedRecords records that arrived but could not be parsed; non-zero means the
 *     producer and this consumer disagree about the JSON, which is the failure that otherwise
 *     looks exactly like "no data"
 * @param staleRecords late windows correctly rejected as older than what is already held
 * @param alertsAbsorbed repeated windows folded into an existing episode instead of raising a
 *     duplicate alert — the deduplication, counted
 * @param newestWindowEnd event time of the freshest measurement; if this stops advancing while
 *     {@code headwayRecords} keeps rising, the stream job is replaying rather than keeping up
 */
public record ServiceStatus(
        Instant now,
        boolean consuming,
        String bootstrapServers,
        List<TopicStatus> topics,
        int routes,
        int vehicles,
        int openAlerts,
        long headwayRecords,
        long vehicleRecords,
        long malformedRecords,
        long staleRecords,
        long alertsOpened,
        long alertsAbsorbed,
        long alertsCleared,
        long framesBroadcast,
        long framesCoalesced,
        int connectedClients,
        Instant newestWindowEnd) {

    public record TopicStatus(String name, boolean running, long received, long malformed,
            String lastError) {
    }
}
