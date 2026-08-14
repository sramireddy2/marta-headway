package dev.headway.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import dev.headway.common.VehiclePosition;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
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

    private VehicleStore store;

    @BeforeEach
    void setUp() {
        store = new VehicleStore();
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
    @DisplayName("vehicles that stopped reporting are evicted")
    void evictsSilentVehicles() {
        Clock fixed = Clock.fixed(T0.plus(Duration.ofMinutes(30)), ZoneOffset.UTC);
        VehicleStore withClock = new VehicleStore(fixed);

        withClock.apply(ping("old-bus", "15", T0));                          // 30 min ago
        withClock.apply(ping("live-bus", "15", T0.plus(Duration.ofMinutes(29)))); // 1 min ago

        assertThat(withClock.evictStale(Duration.ofMinutes(10))).isEqualTo(1);
        assertThat(withClock.snapshot()).containsOnlyKeys("live-bus");
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
     *
     * <p>The {@link CountDownLatch} makes all threads start at the same instant. Without it they
     * begin staggered and rarely collide, so the test would pass even against broken code.
     */
    @Test
    @DisplayName("concurrent scrambled writes still leave the newest reading in place")
    void isSafeUnderConcurrentWrites() throws Exception {
        int threads = 16;
        int pingsPerThread = 500;
        int totalPings = threads * pingsPerThread;

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
                            store.apply(work.get(slice * pingsPerThread + i));
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

        assertThat(store.size()).as("one vehicle, no duplicates").isEqualTo(1);
        assertThat(store.snapshot().get("bus-1").timestamp())
                .as("the newest reading must survive every interleaving")
                .isEqualTo(T0.plusSeconds(totalPings - 1));
        assertThat(store.appliedTotal() + store.staleTotal()).isEqualTo(totalPings);
    }
}
