package dev.headway.gtfs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import com.google.common.collect.ImmutableMap;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(value = 30, unit = TimeUnit.SECONDS)
class GtfsStaticRepositoryTest {

    /** A snapshot carrying just enough identity to tell one load from the next. */
    private static GtfsSnapshot snapshotNumber(int n) {
        return new GtfsSnapshot(
                ImmutableMap.of(), ImmutableMap.of(), ImmutableMap.of(), ImmutableMap.of(),
                ServiceCalendar.permissive(),
                new ScheduleIndex(java.util.Map.of(), ServiceCalendar.permissive()),
                Instant.now(), Instant.ofEpochSecond(n));
    }

    private static int numberOf(GtfsSnapshot snapshot) {
        return (int) snapshot.feedLastModified().getEpochSecond();
    }

    @Test
    @DisplayName("the feed is parsed once and reused, not re-read on every call")
    void loadsOnceAndCaches() {
        AtomicInteger loads = new AtomicInteger();
        try (GtfsStaticRepository repo = new GtfsStaticRepository(
                () -> snapshotNumber(loads.incrementAndGet()),
                Duration.ofHours(6), Duration.ofHours(24))) {

            for (int i = 0; i < 100; i++) {
                assertThat(numberOf(repo.snapshot())).isEqualTo(1);
            }
            assertThat(loads.get()).isEqualTo(1);
        }
    }

    /**
     * Guava guarantees that concurrent callers for a missing key do not all run the loader — one
     * computes and the rest wait on that result. Without it, twenty threads starting together
     * would each kick off a 21 MB download.
     */
    @Test
    @DisplayName("twenty threads racing on a cold cache cause exactly one load")
    void concurrentCallersTriggerOneLoad() throws Exception {
        AtomicInteger loads = new AtomicInteger();
        CountDownLatch startGun = new CountDownLatch(1);
        int threads = 20;

        try (GtfsStaticRepository repo = new GtfsStaticRepository(() -> {
            loads.incrementAndGet();
            Thread.sleep(50); // make the window wide enough to actually race
            return snapshotNumber(1);
        }, Duration.ofHours(6), Duration.ofHours(24));
             ExecutorService pool = Executors.newFixedThreadPool(threads)) {

            CountDownLatch done = new CountDownLatch(threads);
            for (int i = 0; i < threads; i++) {
                pool.submit(() -> {
                    try {
                        startGun.await();
                        repo.snapshot();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        done.countDown();
                    }
                });
            }
            startGun.countDown();
            assertThat(done.await(20, TimeUnit.SECONDS)).isTrue();
        }

        assertThat(loads.get()).as("one load, not twenty").isEqualTo(1);
    }

    /**
     * The whole reason for {@code refreshAfterWrite}.
     *
     * <p>Past the refresh deadline the caller gets the <em>existing</em> snapshot immediately while
     * a reload happens in the background. With {@code expireAfterWrite} alone that caller would sit
     * through a 21 MB download and a 360,000-point parse.
     */
    @Test
    @DisplayName("past the refresh deadline the caller is served immediately, not blocked")
    void refreshDoesNotBlockTheCaller() throws Exception {
        AtomicInteger loads = new AtomicInteger();
        AtomicBoolean firstLoadDone = new AtomicBoolean();

        try (GtfsStaticRepository repo = new GtfsStaticRepository(() -> {
            int n = loads.incrementAndGet();
            if (firstLoadDone.get()) {
                Thread.sleep(1500); // a slow reload
            }
            firstLoadDone.set(true);
            return snapshotNumber(n);
        }, Duration.ofMillis(50), Duration.ofHours(24))) {

            assertThat(numberOf(repo.snapshot())).isEqualTo(1);
            Thread.sleep(120); // let the refresh deadline pass

            long start = System.nanoTime();
            GtfsSnapshot served = repo.snapshot();
            long millis = (System.nanoTime() - start) / 1_000_000;

            assertThat(served).as("served the existing snapshot, stale but instant").isNotNull();
            assertThat(millis)
                    .as("must not have waited for the 1500ms reload")
                    .isLessThan(500);

            // And the reload really does land, eventually.
            await().atMost(15, TimeUnit.SECONDS)
                    .until(() -> numberOf(repo.snapshot()) == 2);
        }
    }

    /**
     * A failed refresh must degrade to "slightly stale", never to "no schedule at all". The
     * agency being briefly unreachable is not a reason to stop resolving trips.
     */
    @Test
    @DisplayName("a failing refresh keeps serving the previous snapshot")
    void failedRefreshKeepsTheOldSnapshot() throws Exception {
        AtomicBoolean shouldFail = new AtomicBoolean();

        try (GtfsStaticRepository repo = new GtfsStaticRepository(() -> {
            if (shouldFail.get()) {
                throw new java.io.IOException("MARTA is down");
            }
            return snapshotNumber(1);
        }, Duration.ofMillis(50), Duration.ofHours(24))) {

            assertThat(numberOf(repo.snapshot())).isEqualTo(1);
            shouldFail.set(true);
            Thread.sleep(120);

            for (int i = 0; i < 10; i++) {
                assertThat(numberOf(repo.snapshot()))
                        .as("still serving the good snapshot despite refresh failures")
                        .isEqualTo(1);
                Thread.sleep(20);
            }
        }
    }

    @Test
    @DisplayName("a failure on the very first load surfaces rather than being swallowed")
    void firstLoadFailurePropagates() {
        try (GtfsStaticRepository repo = new GtfsStaticRepository(
                () -> { throw new java.io.IOException("no feed and no cached copy"); },
                Duration.ofHours(6), Duration.ofHours(24))) {

            assertThatThrownBy(repo::snapshot)
                    .isInstanceOf(IllegalStateException.class)
                    .hasRootCauseMessage("no feed and no cached copy");
        }
    }

    @Test
    @DisplayName("invalidate() forces the next call to reload")
    void invalidateForcesReload() {
        AtomicInteger loads = new AtomicInteger();
        try (GtfsStaticRepository repo = new GtfsStaticRepository(
                () -> snapshotNumber(loads.incrementAndGet()),
                Duration.ofHours(6), Duration.ofHours(24))) {

            assertThat(numberOf(repo.snapshot())).isEqualTo(1);
            repo.invalidate();
            assertThat(numberOf(repo.snapshot())).isEqualTo(2);
        }
    }
}
