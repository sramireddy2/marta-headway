package dev.headway.gtfs;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Serves the current {@link GtfsSnapshot}, keeping it fresh without ever making a caller wait.
 *
 * <h2>Why a cache at all for a single value</h2>
 *
 * The schedule is not static in the "never changes" sense — agencies republish it when service
 * changes, typically every few weeks. A service that parses it once at startup is serving a
 * schedule that silently drifts out of date until someone restarts it, and the symptom is
 * unresolvable trip ids rather than an error.
 *
 * <p>So this holds one entry in a Guava {@link LoadingCache}: computed on first use, thread-safe
 * without any locking of ours, and reloaded on a timer.
 *
 * <h2>refreshAfterWrite versus expireAfterWrite — the distinction that matters</h2>
 *
 * <ul>
 *   <li><b>expireAfterWrite</b> makes the entry <em>invalid</em> at the deadline. The unlucky
 *       caller who arrives next blocks while the value is recomputed. Recomputing here means a
 *       21 MB download and parsing 360,000 shape points, so that caller waits seconds.
 *   <li><b>refreshAfterWrite</b> keeps serving the existing value and reloads in the background.
 *       Nobody blocks. Exactly one thread does the work; everyone else gets the slightly stale
 *       snapshot, which for a bus schedule is entirely fine.
 * </ul>
 *
 * Both are set. Refresh at 6 hours is the normal path. Expire at 24 hours is the backstop: if
 * refreshes keep failing — MARTA down, disk full — the entry eventually goes invalid rather than
 * letting us serve a week-old schedule forever while insisting everything is fine.
 *
 * <h2>The subtlety in reload()</h2>
 *
 * Guava's default {@link CacheLoader#reload} is <b>synchronous</b>: it just calls {@code load()} on
 * the thread that triggered the refresh. So a plain {@code refreshAfterWrite} still blocks
 * somebody — the very thing it looks like it prevents. Making refresh genuinely asynchronous
 * requires overriding {@code reload} to return a future, which is what happens below.
 */
public final class GtfsStaticRepository implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(GtfsStaticRepository.class);

    /** The cache holds exactly one entry; this is its key. */
    private static final String KEY = "static-feed";

    public static final Duration DEFAULT_REFRESH_AFTER = Duration.ofHours(6);
    public static final Duration DEFAULT_EXPIRE_AFTER = Duration.ofHours(24);

    /**
     * Where a snapshot comes from. One method, so a test can supply a canned snapshot, a counting
     * source, or one that fails — with no network, no zip and no mocking framework.
     */
    @FunctionalInterface
    public interface SnapshotSource {
        GtfsSnapshot loadSnapshot() throws Exception;
    }

    private final LoadingCache<String, GtfsSnapshot> cache;
    private final ExecutorService reloadExecutor;

    public GtfsStaticRepository(GtfsFeedDownloader downloader, GtfsStaticLoader loader) {
        this(sourceOf(downloader, loader), DEFAULT_REFRESH_AFTER, DEFAULT_EXPIRE_AFTER);
    }

    /** Download-then-parse, as one step. */
    public static SnapshotSource sourceOf(GtfsFeedDownloader downloader, GtfsStaticLoader loader) {
        return () -> {
            GtfsFeedDownloader.Download download = downloader.fetch();
            return loader.load(download.zip(), download.lastModified());
        };
    }

    public GtfsStaticRepository(SnapshotSource source, Duration refreshAfter, Duration expireAfter) {

        // One daemon thread. Reloads are rare and must never run concurrently with each other;
        // a second parallel download of the same 21 MB file would be pure waste.
        this.reloadExecutor = Executors.newSingleThreadExecutor(runnable -> {
            Thread t = new Thread(runnable, "gtfs-reload");
            t.setDaemon(true);
            return t;
        });

        CacheLoader<String, GtfsSnapshot> cacheLoader = new CacheLoader<>() {
            @Override
            public GtfsSnapshot load(String key) throws Exception {
                return source.loadSnapshot();
            }

            @Override
            public ListenableFuture<GtfsSnapshot> reload(String key, GtfsSnapshot current) {
                ListenableFutureTask<GtfsSnapshot> task = ListenableFutureTask.create(() -> {
                    try {
                        GtfsSnapshot refreshed = load(key);
                        log.info("Refreshed static GTFS: {}", refreshed);
                        return refreshed;
                    } catch (Exception e) {
                        // Returning the current snapshot rather than throwing keeps the cache
                        // populated. A failed refresh should degrade to "slightly stale", not to
                        // "no schedule at all" - the expireAfterWrite backstop is what stops that
                        // being indefinite.
                        log.warn("Static GTFS refresh failed; keeping the previous snapshot", e);
                        return current;
                    }
                });
                reloadExecutor.execute(task);
                return task;
            }
        };

        this.cache = CacheBuilder.newBuilder()
                .maximumSize(1)
                .refreshAfterWrite(refreshAfter)
                .expireAfterWrite(expireAfter)
                .build(cacheLoader);
    }

    /** Convenience wiring for MARTA with an on-disk cache directory. */
    public static GtfsStaticRepository marta(Path cacheDir) {
        return new GtfsStaticRepository(GtfsFeedDownloader.marta(cacheDir), new GtfsStaticLoader());
    }

    /**
     * The current snapshot, downloading and parsing on the very first call.
     *
     * <p>Only the first caller ever waits. Guava guarantees that concurrent callers for a missing
     * key do not all run the loader — one computes, the rest block on that one result.
     */
    public GtfsSnapshot snapshot() {
        try {
            return cache.get(KEY);
        } catch (ExecutionException e) {
            throw new IllegalStateException("Could not load the static GTFS feed", e.getCause());
        }
    }

    /** Forces a reload on the next call. Mainly useful in tests and for a manual refresh endpoint. */
    public void invalidate() {
        cache.invalidate(KEY);
    }

    /** Hit and miss counts, useful once this is behind an API. */
    public com.google.common.cache.CacheStats stats() {
        return cache.stats();
    }

    @Override
    public void close() {
        reloadExecutor.shutdown();
        try {
            if (!reloadExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                reloadExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            reloadExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
