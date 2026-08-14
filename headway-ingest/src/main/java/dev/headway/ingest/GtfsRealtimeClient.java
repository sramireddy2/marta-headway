package dev.headway.ingest;

import com.google.transit.realtime.GtfsRealtime.FeedEntity;
import com.google.transit.realtime.GtfsRealtime.FeedMessage;
import com.google.transit.realtime.GtfsRealtime.Position;
import com.google.transit.realtime.GtfsRealtime.TripDescriptor;
import dev.headway.common.VehiclePosition;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Fetches a GTFS-Realtime VehiclePositions feed and turns it into domain objects.
 *
 * <p>GTFS-Realtime is Protocol Buffers ("protobuf") — a compact <em>binary</em> format defined by a
 * schema file. You cannot open it in a text editor. The {@code gtfs-realtime-bindings} dependency
 * ships Java classes generated from Google's official schema, so decoding is a single call:
 * {@link FeedMessage#parseFrom(InputStream)}.
 *
 * <p>Note the deliberate split below: {@link #fetch()} does I/O, {@link #toVehiclePositions} is a
 * pure function with no network in it. That split is why the parsing logic can be unit-tested
 * against a recorded feed file, with no internet connection and no flakiness.
 *
 * <p>This class is immutable and therefore thread-safe. {@link HttpClient} is itself thread-safe
 * and is designed to be shared — creating one per request would leak connection pools.
 */
public final class GtfsRealtimeClient {

    private static final Logger log = LoggerFactory.getLogger(GtfsRealtimeClient.class);

    /** MARTA's public, no-API-key-required vehicle position feed. */
    public static final URI MARTA_VEHICLE_POSITIONS =
            URI.create("https://gtfs-rt.itsmarta.com/TMGTFSRealTimeWebService/vehicle/vehiclepositions.pb");

    private final HttpClient httpClient;
    private final URI feedUrl;
    private final Duration requestTimeout;

    public GtfsRealtimeClient(URI feedUrl) {
        this(feedUrl, Duration.ofSeconds(10));
    }

    public GtfsRealtimeClient(URI feedUrl, Duration requestTimeout) {
        this.feedUrl = feedUrl;
        this.requestTimeout = requestTimeout;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build();
    }

    /**
     * Downloads the feed and decodes it.
     *
     * @throws IOException on a network failure, a non-200 response, or malformed protobuf
     */
    public FeedMessage fetch() throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder(feedUrl)
                .timeout(requestTimeout)
                .header("User-Agent", "headway/0.1 (transit bunching detector)")
                .GET()
                .build();

        HttpResponse<byte[]> response =
                httpClient.send(request, HttpResponse.BodyHandlers.ofByteArray());

        if (response.statusCode() != 200) {
            throw new IOException("Feed returned HTTP " + response.statusCode() + " for " + feedUrl);
        }

        byte[] body = response.body();
        log.debug("Fetched {} bytes from {}", body.length, feedUrl);
        return FeedMessage.parseFrom(body);
    }

    /**
     * Converts a decoded feed into our own domain objects, skipping entries we cannot use.
     *
     * <p>Protobuf has no concept of {@code null}: every field has a zero-value default, so
     * {@code getRouteId()} returns {@code ""} rather than {@code null} when the agency omitted it.
     * That is why real code always pairs a getter with its {@code hasX()} companion. Silently
     * treating a missing route as the empty-string route would corrupt every headway downstream.
     */
    public static List<VehiclePosition> toVehiclePositions(FeedMessage feed) {
        Instant feedTimestamp = Instant.ofEpochSecond(feed.getHeader().getTimestamp());
        List<VehiclePosition> out = new ArrayList<>(feed.getEntityCount());

        for (FeedEntity entity : feed.getEntityList()) {
            if (!entity.hasVehicle()) {
                continue; // This entity is a trip update or an alert, not a position.
            }
            com.google.transit.realtime.GtfsRealtime.VehiclePosition vp = entity.getVehicle();

            if (!vp.hasPosition()) {
                continue; // No GPS fix; nothing we can place on a map.
            }
            Position position = vp.getPosition();

            // Prefer the vehicle's own id; fall back to the feed entity id.
            String vehicleId = vp.hasVehicle() && !vp.getVehicle().getId().isEmpty()
                    ? vp.getVehicle().getId()
                    : entity.getId();

            TripDescriptor trip = vp.getTrip();
            String routeId = trip.getRouteId();
            if (routeId.isEmpty()) {
                continue; // Headway is a per-route concept. Unroutable ping.
            }

            // The vehicle's own timestamp is when the GPS was read. Only if the agency omits it do
            // we fall back to the feed timestamp, which is merely when the feed was assembled.
            Instant timestamp = vp.hasTimestamp() && vp.getTimestamp() > 0
                    ? Instant.ofEpochSecond(vp.getTimestamp())
                    : feedTimestamp;

            out.add(new VehiclePosition(
                    vehicleId,
                    routeId,
                    trip.hasTripId() ? trip.getTripId() : null,
                    trip.hasDirectionId() ? trip.getDirectionId() : null,
                    position.getLatitude(),
                    position.getLongitude(),
                    position.hasBearing() ? position.getBearing() : null,
                    position.hasSpeed() ? position.getSpeed() : null,
                    timestamp));
        }
        return out;
    }

    /** Convenience: fetch and convert in one call. */
    public List<VehiclePosition> fetchVehiclePositions() throws IOException, InterruptedException {
        return toVehiclePositions(fetch());
    }

    public URI feedUrl() {
        return feedUrl;
    }
}
