package dev.headway.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import dev.headway.common.VehiclePosition;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class VehicleStoreTest {

    private static final Instant T0 = Instant.parse("2026-08-14T21:00:00Z");
    private static final Duration MAX_AGE = Duration.ofMinutes(10);

    private VehicleStore store;

    /**
     * A clock the test drives by hand.
     *
     * <p>Two reasons this beats {@code Instant.now()}. The store now rejects readings older than
     * {@code maxAge}, so a test using a hard-coded date would start failing ten minutes after it
     * was written and never pass again. And testing eviction needs time to <em>pass</em>, which
     * otherwise means {@code Thread.sleep} — slow, and flaky on a loaded CI box.
     */
    private static final class TestClock extends Clock {
        private Instant now;

        TestClock(Instant now) {
            this.now = now;
        }

        void advance(Duration by) {
            now = now.plus(by);
        }

        @Override
        public Instant instant() {
            return now;
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }
    }

    @BeforeEach
    void setUp() {
        store = new VehicleStore(Clock.fixed(T0, ZoneOffset.UTC), MAX_AGE);
    }

    private static VehiclePosition ping(String vehicleId, String routeId, Instant at) {
        return new VehiclePosition(vehicleId, routeId, "trip-1", 0,
                33.75, -84.39, null, null, at);
    }

    @Test
    @DisplayName("a first sighting is NEW, a newer reading is UPDATED")
    void tracksNewAndUpdated() {
        assertThat(store.apply(ping("bus-1", "15", T0))).isEqualTo(VehicleStore.Outcome.NEW);
        assertThat(store.apply(ping("bus-1", "15", T0.plusSeconds(15))))
                .isEqualTo(VehicleStore.Outcome.UPDATED);
        assertThat(store.size()).isEqualTo(1);
    }

    @Test
    @DisplayName("re-delivering the same message changes nothing (idempotency)")
    void isIdempotent() {
        VehiclePosition p = ping("bus-1", "15", T0);

        assertThat(store.apply(p)).isEqualTo(VehicleStore.Outcome.NEW);
        for (int i = 0; i < 100; i++) {
            assertThat(store.apply(p)).isEqualTo(VehicleStore.Outcome.STALE);
        }

        assertThat(store.size()).isEqualTo(1);
        assertThat(store.snapshot().get("bus-1").timestamp()).isEqualTo(T0);
    }

    @Test
    @DisplayName("an out-of-order older reading never overwrites a newer one")
    void rejectsOutOfOrderReadings() {
        store.apply(ping("bus-1", "15", T0.plusSeconds(60)));

        assertThat(store.apply(ping("bus-1", "15", T0))).isEqualTo(VehicleStore.Outcome.STALE);
        assertThat(store.snapshot().get("bus-1").timestamp()).isEqualTo(T0.plusSeconds(60));
    }

    /**
     * The lock-free guard in front of {@code compute} answers STALE without taking the bin lock,
     * which is worth 7.5x on the path that carries about half of real traffic. These pin the two ways a
     * shortcut like that goes wrong: answering STALE when it should not, and leaving the store in a
     * state where the next genuine update is refused.
     */
    @Test
    @DisplayName("the lock-free stale check never blocks a later genuine update")
    void fastPathDoesNotPoisonTheStore() {
        assertThat(store.apply(ping("bus-1", "15", T0.plusSeconds(60))))
                .isEqualTo(VehicleStore.Outcome.NEW);

        // Fifty stale re-sends, every one short-circuited before the lock.
        for (int i = 0; i < 50; i++) {
            assertThat(store.apply(ping("bus-1", "15", T0.plusSeconds(i))))
                    .isEqualTo(VehicleStore.Outcome.STALE);
        }

        // ...and the store still accepts the next real reading, and still holds the right one.
        assertThat(store.apply(ping("bus-1", "15", T0.plusSeconds(61))))
                .isEqualTo(VehicleStore.Outcome.UPDATED);
        assertThat(store.snapshot().get("bus-1").timestamp()).isEqualTo(T0.plusSeconds(61));
        assertThat(store.size()).isEqualTo(1);
    }

    /**
     * An equal timestamp is <em>not</em> newer, so it must be STALE — the guard uses
     * "prove it is not newer", and an off-by-one there would let a duplicate through and make
     * replaying a Kafka topic non-idempotent.
     */
    @Test
    @DisplayName("a reading with an identical timestamp is stale, not an update")
    void equalTimestampIsStale() {
        store.apply(ping("bus-1", "15", T0));

        assertThat(store.apply(ping("bus-1", "15", T0))).isEqualTo(VehicleStore.Outcome.STALE);
        assertThat(store.staleTotal()).isEqualTo(1);
    }

    /** After eviction the key is gone, so the guard must fall through rather than answer STALE. */
    @Test
    @DisplayName("a vehicle re-appearing after eviction is NEW again")
    void fastPathDoesNotHideAnEvictedVehicle() {
        TestClock clock = new TestClock(T0);
        VehicleStore evicting = new VehicleStore(clock, MAX_AGE);
        evicting.apply(ping("bus-1", "15", T0));

        clock.advance(MAX_AGE.plusMinutes(1));
        assertThat(evicting.evictStale()).isEqualTo(1);

        assertThat(evicting.apply(ping("bus-1", "15", clock.instant())))
                .isEqualTo(VehicleStore.Outcome.NEW);
    }

    @Test
    @DisplayName("snapshots are immutable and do not change under later writes")
    void snapshotsAreFrozen() {
        store.apply(ping("bus-1", "15", T0));
        var snapshot = store.snapshot();

        store.apply(ping("bus-2", "15", T0));

        assertThat(snapshot).hasSize(1);
        assertThat(store.size()).isEqualTo(2);
    }

    @Test
    @DisplayName("onRoute filters to one route")
    void filtersByRoute() {
        store.apply(ping("bus-1", "15", T0));
        store.apply(ping("bus-2", "15", T0));
        store.apply(ping("bus-3", "110", T0));

        assertThat(store.onRoute("15")).extracting(VehiclePosition::vehicleId)
                .containsExactly("bus-1", "bus-2");
        assertThat(store.routeCount()).isEqualTo(2);
    }

    @Test
    @DisplayName("vehicles that go silent are evicted once they age past maxAge")
    void evictsSilentVehicles() {
        TestClock clock = new TestClock(T0);
        VehicleStore withClock = new VehicleStore(clock, MAX_AGE);

        withClock.apply(ping("goes-quiet", "15", T0));
        clock.advance(Duration.ofMinutes(20));
        withClock.apply(ping("still-live", "15", T0.plus(Duration.ofMinutes(20))));

        assertThat(withClock.evictStale()).isEqualTo(1);
        assertThat(withClock.snapshot()).containsOnlyKeys("still-live");
    }

    // ---------------------------------------------------------------------------------------
    // Admission control. These cover the bug found by running the service against the live feed:
    // a bus with a frozen GPS stayed in MARTA's feed with an unchanging timestamp, so the sweep
    // evicted it and the very next poll re-added it as NEW, once a minute, forever.
    // ---------------------------------------------------------------------------------------

    @Test
    @DisplayName("a reading already older than maxAge is refused, not stored")
    void refusesReadingsOlderThanMaxAge() {
        // now is T0; this reading is 12 minutes stale, past the 10 minute limit.
        assertThat(store.apply(ping("frozen-gps", "96", T0.minus(Duration.ofMinutes(12)))))
                .isEqualTo(VehicleStore.Outcome.REJECTED);

        assertThat(store.size()).isZero();
        assertThat(store.rejectedTotal()).isEqualTo(1);
    }

    @Test
    @DisplayName("a future-dated reading is refused, so it can never become unevictable")
    void refusesFutureDatedReadings() {
        // A transponder with a clock an hour fast. If admitted, no real reading would ever look
        // newer, and it would never fall past the eviction cutoff either.
        assertThat(store.apply(ping("bad-clock", "15", T0.plus(Duration.ofHours(1)))))
                .isEqualTo(VehicleStore.Outcome.REJECTED);

        assertThat(store.size()).isZero();
    }

    @Test
    @DisplayName("small clock skew is tolerated rather than treated as an error")
    void toleratesSmallClockSkew() {
        assertThat(store.apply(ping("slightly-fast", "15", T0.plusSeconds(30))))
                .isEqualTo(VehicleStore.Outcome.NEW);
    }

    /**
     * The invariant the fix establishes, stated directly.
     *
     * <p>Admission and eviction read the same {@code maxAge} off the same object, so a reading the
     * store accepts can never be one the very next sweep throws away. Before the fix this test
     * failed: the frozen-GPS ping was admitted as {@code NEW} and then immediately evicted.
     */
    @Test
    @DisplayName("anything admitted survives an immediate sweep")
    void admittedReadingsSurviveTheNextSweep() {
        TestClock clock = new TestClock(T0.plus(Duration.ofMinutes(12)));
        VehicleStore s = new VehicleStore(clock, MAX_AGE);

        List<VehiclePosition> feed = List.of(
                ping("frozen-gps", "96", T0),                              // 12 min stale
                ping("healthy", "15", T0.plus(Duration.ofMinutes(12))),    // current
                ping("bad-clock", "3", T0.plus(Duration.ofHours(2))));     // future-dated

        VehicleStore.Stats stats = s.applyAll(feed);

        assertThat(stats.created()).isEqualTo(1);
        assertThat(stats.rejected()).isEqualTo(2);
        assertThat(stats.total()).isEqualTo(3);

        int sizeBefore = s.size();
        assertThat(s.evictStale()).as("nothing admitted should be immediately evictable").isZero();
        assertThat(s.size()).isEqualTo(sizeBefore);
        assertThat(s.snapshot()).containsOnlyKeys("healthy");
    }

    @Test
    @DisplayName("a frozen-GPS vehicle does not oscillate between evicted and re-added")
    void frozenVehicleDoesNotChurn() {
        TestClock clock = new TestClock(T0.plus(Duration.ofMinutes(12)));
        VehicleStore s = new VehicleStore(clock, MAX_AGE);
        VehiclePosition frozen = ping("frozen-gps", "96", T0);

        // Simulate several poll/sweep cycles against a feed that keeps re-sending the same ping.
        for (int cycle = 0; cycle < 5; cycle++) {
            s.apply(frozen);
            s.evictStale();
            clock.advance(Duration.ofMinutes(1));
        }

        assertThat(s.size()).isZero();
        assertThat(s.evictedTotal()).as("never admitted, so never evicted").isZero();
        assertThat(s.rejectedTotal()).isEqualTo(5);
    }

    /**
     * The test that actually justifies {@code compute()}.
     *
     * <p>Many threads slam the same vehicle with readings in scrambled order. Whatever the
     * interleaving, the newest timestamp must win.
     *
     * <p>This is not a hypothetical. The same workload run against a check-then-act
     * {@code get()}-then-{@code put()} implementation left a stale position in the map on
     * <b>5 of 40 runs</b> on this machine. That ~12% hit rate is exactly what makes the bug
     * dangerous: it passes on your laptop, passes in CI, and corrupts data in production.
     */
    @Test
    @DisplayName("concurrent scrambled writes still leave the newest reading in place")
    void isSafeUnderConcurrentWrites() throws Exception {
        int threads = 16;
        int pingsPerThread = 500;
        int totalPings = threads * pingsPerThread;

        // The readings span ~2.2 hours, so the clock sits at the end of that range and maxAge is
        // wide open. This test is about write ordering, not admission control.
        VehicleStore raceStore = new VehicleStore(
                Clock.fixed(T0.plusSeconds(totalPings), ZoneOffset.UTC), Duration.ofDays(1));

        // Every possible timestamp, shuffled, so no thread gets a tidy ascending run.
        List<VehiclePosition> work = new ArrayList<>(totalPings);
        for (int i = 0; i < totalPings; i++) {
            work.add(ping("bus-1", "15", T0.plusSeconds(i)));
        }
        Collections.shuffle(work);

        CountDownLatch startGun = new CountDownLatch(1);
        CountDownLatch finished = new CountDownLatch(threads);

        try (ExecutorService pool = Executors.newFixedThreadPool(threads)) {
            for (int t = 0; t < threads; t++) {
                int slice = t;
                pool.submit(() -> {
                    try {
                        startGun.await();
                        for (int i = 0; i < pingsPerThread; i++) {
                            raceStore.apply(work.get(slice * pingsPerThread + i));
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        finished.countDown();
                    }
                });
            }

            startGun.countDown();
            assertThat(finished.await(30, TimeUnit.SECONDS)).as("all threads finished").isTrue();
        }

        assertThat(raceStore.size()).as("one vehicle, no duplicates").isEqualTo(1);
        assertThat(raceStore.snapshot().get("bus-1").timestamp())
                .as("the newest reading must survive every interleaving")
                .isEqualTo(T0.plusSeconds(totalPings - 1));
        assertThat(raceStore.rejectedTotal()).as("nothing should be refused here").isZero();
        assertThat(raceStore.appliedTotal() + raceStore.staleTotal()).isEqualTo(totalPings);
    }
}
