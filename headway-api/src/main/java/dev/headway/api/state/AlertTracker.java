package dev.headway.api.state;

import dev.headway.api.model.AlertEpisode;
import dev.headway.api.model.RouteHeadway;
import dev.headway.gtfs.HeadwayStatus;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

/**
 * Collapses a stream of repeated window measurements into distinct alert events.
 *
 * <p>This is the fix for the duplicate-alert limitation step 8 left open. Sliding windows report
 * the same three-minute bunching event six or more times; this class turns those six records into
 * one {@link AlertEpisode} that opens, runs, and closes. See that class for why the fix belongs
 * here and not in the Spark job.
 *
 * <h2>Thread safety</h2>
 *
 * Open episodes live in a {@link ConcurrentHashMap} and are only ever mutated through {@code
 * compute} / {@code remove}, so a read-modify-write of one route's episode is atomic even though
 * the consumer thread and the sweeper run concurrently.
 *
 * <p>Closing an episode is deliberately two operations — {@code remove} from the open map, then
 * append to history — and never one nested inside the other. Appending to the history deque takes
 * its own lock, and taking that lock while holding a {@code ConcurrentHashMap} bin lock is how
 * deadlocks get built. {@code remove} returning non-null already guarantees exactly one thread owns
 * the episode, so nothing is lost by doing the archive afterwards.
 *
 * <p>Declared as a {@code @Bean} in {@code HeadwayApiApplication} rather than annotated
 * {@code @Component}, because its history bound comes from configuration.
 */
public final class AlertTracker {

    /** What one measurement did to the alert picture. */
    public enum Change {
        /** A route that was fine is now not. Worth telling someone about. */
        OPENED,
        /** An already-known problem got worse. Also worth telling someone about. */
        ESCALATED,
        /** More evidence for a problem already reported. This is the case being deduplicated. */
        CONTINUED,
        /** A route recovered. */
        CLEARED,
        /** Nothing alertable, and nothing was open. */
        NONE
    }

    /** @param episode the episode this measurement affected, or null for {@link Change#NONE} */
    public record Observation(Change change, AlertEpisode episode) {

        static final Observation NOTHING = new Observation(Change.NONE, null);

        /** Whether a human should be shown this, as opposed to it merely updating a count. */
        public boolean isNotable() {
            return change == Change.OPENED || change == Change.ESCALATED;
        }
    }

    private static final int DEFAULT_HISTORY = 200;

    private final ConcurrentHashMap<String, AlertEpisode> open = new ConcurrentHashMap<>();
    private final Deque<AlertEpisode> history = new ArrayDeque<>();
    private final int historyLimit;

    private final LongAdder opened = new LongAdder();
    private final LongAdder absorbed = new LongAdder();
    private final LongAdder cleared = new LongAdder();

    public AlertTracker() {
        this(DEFAULT_HISTORY);
    }

    public AlertTracker(int historyLimit) {
        this.historyLimit = historyLimit;
    }

    /** Feeds one measurement in and reports what it changed. */
    public Observation observe(RouteHeadway headway) {
        if (headway == null || headway.headwayGroup() == null || headway.windowEnd() == null) {
            return Observation.NOTHING;
        }
        if (!headway.isAlertable()) {
            return close(headway.headwayGroup(), headway.windowEnd())
                    .map(episode -> new Observation(Change.CLEARED, episode))
                    .orElse(Observation.NOTHING);
        }

        AtomicReference<Change> change = new AtomicReference<>(Change.CONTINUED);
        AlertEpisode result = open.compute(headway.headwayGroup(), (group, existing) -> {
            if (existing == null) {
                change.set(Change.OPENED);
                return AlertEpisode.opening(headway);
            }
            // Out-of-order window: a measurement older than what this episode already absorbed
            // adds nothing and would drag "current status" backwards.
            if (headway.windowEnd().isBefore(existing.lastSeenAt())) {
                return existing;
            }
            HeadwayStatus before = existing.worstStatus();
            AlertEpisode updated = existing.absorb(headway);
            if (updated.worstStatus() != before) {
                change.set(Change.ESCALATED);
            }
            return updated;
        });

        switch (change.get()) {
            case OPENED -> opened.increment();
            default -> absorbed.increment();
        }
        return new Observation(change.get(), result);
    }

    /**
     * Ends episodes for routes that have stopped reporting.
     *
     * <p>A route recovering sends an ON_SCHEDULE window, which closes its episode through {@link
     * #observe}. A route whose buses go out of service sends nothing at all, and without this sweep
     * its episode would stay open on the dashboard forever, describing a problem that no longer has
     * any vehicles involved in it.
     *
     * @param linger how long silence is tolerated before an episode is presumed over
     * @return the episodes that were closed
     */
    public List<AlertEpisode> sweep(Instant now, Duration linger) {
        Instant cutoff = now.minus(linger);
        List<AlertEpisode> closed = new ArrayList<>();
        for (Map.Entry<String, AlertEpisode> entry : open.entrySet()) {
            if (entry.getValue().lastSeenAt().isBefore(cutoff)) {
                close(entry.getKey(), entry.getValue().lastSeenAt()).ifPresent(closed::add);
            }
        }
        return closed;
    }

    private Optional<AlertEpisode> close(String group, Instant at) {
        AlertEpisode episode = open.remove(group);
        if (episode == null) {
            return Optional.empty();
        }
        AlertEpisode ended = episode.closing(at);
        archive(ended);
        cleared.increment();
        return Optional.of(ended);
    }

    private void archive(AlertEpisode episode) {
        synchronized (history) {
            history.addFirst(episode);
            while (history.size() > historyLimit) {
                history.removeLast();
            }
        }
    }

    /** Everything currently going wrong, worst first. */
    public List<AlertEpisode> openEpisodes() {
        return open.values().stream()
                .sorted(Comparator
                        .comparing((AlertEpisode e) -> severityOf(e.worstStatus())).reversed()
                        .thenComparing(AlertEpisode::startedAt))
                .toList();
    }

    /** Recently finished episodes, newest first. */
    public List<AlertEpisode> recentHistory(int limit) {
        synchronized (history) {
            return history.stream().limit(limit).toList();
        }
    }

    private static int severityOf(HeadwayStatus status) {
        if (status == null) {
            return 0;
        }
        return switch (status) {
            case SEVERE_BUNCHING, SEVERE_GAPPING -> 2;
            case BUNCHING, GAPPING -> 1;
            case ON_SCHEDULE, LAYOVER -> 0;
        };
    }

    public int openCount() {
        return open.size();
    }

    public long openedTotal() {
        return opened.sum();
    }

    /**
     * How many window records were folded into an already-open episode instead of raising a new
     * alert. This number <em>is</em> the deduplication, measured.
     */
    public long absorbedTotal() {
        return absorbed.sum();
    }

    public long clearedTotal() {
        return cleared.sum();
    }
}
