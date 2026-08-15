package dev.headway.gtfs;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Fetches {@code google_transit.zip}, keeping a copy on disk so restarts are free.
 *
 * <p>The zip is 21 MB and MARTA republishes it every few weeks — the copy we measured was seven
 * weeks old. Downloading it on every start would be 21 MB of someone else's bandwidth to receive
 * bytes we already have.
 *
 * <p>So the file is cached under {@code data/gtfs/} and re-fetched with a <b>conditional GET</b>:
 * we send {@code If-Modified-Since} with the timestamp of what we already hold, and the server
 * answers {@code 304 Not Modified} with an empty body if nothing changed. MARTA serves
 * {@code Last-Modified} (no {@code ETag}), so that is the validator we use.
 *
 * <p>A 304 costs a few hundred bytes instead of 21 MB, and — unlike a "re-download once a day"
 * timer — it is still correct if the agency publishes twice in one day or not at all for a month.
 */
public final class GtfsFeedDownloader {

    private static final Logger log = LoggerFactory.getLogger(GtfsFeedDownloader.class);

    public static final URI MARTA_STATIC_FEED =
            URI.create("https://www.itsmarta.com/google_transit_feed/google_transit.zip");

    /** HTTP dates are RFC 1123 and always GMT, regardless of anyone's local zone. */
    private static final DateTimeFormatter HTTP_DATE =
            DateTimeFormatter.RFC_1123_DATE_TIME.withZone(ZoneOffset.UTC);

    private final URI feedUrl;
    private final Path cacheDir;
    private final HttpClient httpClient;

    public GtfsFeedDownloader(URI feedUrl, Path cacheDir) {
        this.feedUrl = feedUrl;
        this.cacheDir = cacheDir;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .followRedirects(HttpClient.Redirect.NORMAL) // the feed URL 301s
                .build();
    }

    public static GtfsFeedDownloader marta(Path cacheDir) {
        return new GtfsFeedDownloader(MARTA_STATIC_FEED, cacheDir);
    }

    /** Where the zip lives locally. */
    public Path zipPath() {
        return cacheDir.resolve("google_transit.zip");
    }

    /** Sidecar holding the {@code Last-Modified} of whatever is in {@link #zipPath()}. */
    private Path stampPath() {
        return cacheDir.resolve("google_transit.lastmodified");
    }

    /**
     * Returns a local zip, downloading only if the server has something newer.
     *
     * @return the local path plus the feed's publication time
     */
    public Download fetch() throws IOException, InterruptedException {
        Files.createDirectories(cacheDir);
        Optional<String> cachedStamp = readStamp();

        HttpRequest.Builder request = HttpRequest.newBuilder(feedUrl)
                .timeout(Duration.ofMinutes(3)) // 21 MB on a slow connection
                .header("User-Agent", "headway/0.1 (transit bunching detector)")
                .GET();

        boolean haveLocalCopy = Files.isRegularFile(zipPath()) && Files.size(zipPath()) > 0;
        if (haveLocalCopy && cachedStamp.isPresent()) {
            request.header("If-Modified-Since", cachedStamp.get());
        }

        // Stream straight to a temp file. Buffering 21 MB in a byte[] first would work but is
        // pointless memory pressure for something headed to disk anyway.
        Path temp = Files.createTempFile(cacheDir, "gtfs-", ".part");
        HttpResponse<Path> response;
        try {
            response = httpClient.send(request.build(),
                    HttpResponse.BodyHandlers.ofFile(temp, java.nio.file.StandardOpenOption.WRITE,
                            java.nio.file.StandardOpenOption.TRUNCATE_EXISTING));
        } catch (IOException | InterruptedException e) {
            Files.deleteIfExists(temp);
            if (haveLocalCopy) {
                // A network blip should not take the service down when we already have a usable
                // schedule on disk. Stale reference data beats none.
                log.warn("Could not reach {} ({}). Using the cached copy.", feedUrl, e.toString());
                return new Download(zipPath(), parseStamp(cachedStamp), false);
            }
            throw e;
        }

        int status = response.statusCode();
        if (status == 304) {
            Files.deleteIfExists(temp);
            log.info("Static feed unchanged (304); using cached {}", zipPath());
            return new Download(zipPath(), parseStamp(cachedStamp), false);
        }
        if (status != 200) {
            Files.deleteIfExists(temp);
            throw new IOException("Static feed returned HTTP " + status + " for " + feedUrl);
        }

        String lastModified = response.headers().firstValue("Last-Modified").orElse(null);

        // Move into place only after a complete download, so an interrupted run can never leave a
        // truncated zip that the next start would try to parse.
        Files.move(temp, zipPath(), StandardCopyOption.REPLACE_EXISTING);
        if (lastModified != null) {
            Files.writeString(stampPath(), lastModified, StandardCharsets.UTF_8);
        }

        log.info("Downloaded static feed: {} ({} bytes), published {}",
                zipPath(), Files.size(zipPath()), lastModified);
        return new Download(zipPath(), parseStamp(Optional.ofNullable(lastModified)), true);
    }

    private Optional<String> readStamp() throws IOException {
        Path stamp = stampPath();
        if (!Files.isRegularFile(stamp)) {
            return Optional.empty();
        }
        String value = Files.readString(stamp, StandardCharsets.UTF_8).trim();
        return value.isEmpty() ? Optional.empty() : Optional.of(value);
    }

    private static Instant parseStamp(Optional<String> httpDate) {
        return httpDate.map(value -> {
            try {
                return Instant.from(HTTP_DATE.parse(value));
            } catch (RuntimeException e) {
                log.warn("Unparseable Last-Modified '{}'", value);
                return Instant.EPOCH;
            }
        }).orElse(Instant.EPOCH);
    }

    /**
     * @param zip local path to the feed
     * @param lastModified when the agency published it
     * @param freshlyDownloaded false when a 304 or a network failure meant we reused the disk copy
     */
    public record Download(Path zip, Instant lastModified, boolean freshlyDownloaded) {}
}
