package dev.headway.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import dev.headway.api.web.Conflator;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** The handoff that drops stale snapshots instead of queueing them. */
@Timeout(30)
class ConflatorTest {

    @Test
    @DisplayName("everything submitted while the sink is busy collapses to the newest value")
    void coalescesUnderLoad() throws Exception {
        CountDownLatch release = new CountDownLatch(1);
        CopyOnWriteArrayList<Integer> delivered = new CopyOnWriteArrayList<>();
        AtomicInteger started = new AtomicInteger();

        try (Conflator<Integer> conflator = new Conflator<>("test", value -> {
            if (started.incrementAndGet() == 1) {
                try {
                    release.await();          // hold the worker inside the first delivery
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            delivered.add(value);
        })) {

            conflator.submit(1);
            await().atMost(Duration.ofSeconds(5)).until(() -> started.get() == 1);

            // These all land while the worker is blocked on the latch.
            for (int i = 2; i <= 100; i++) {
                conflator.submit(i);
            }
            release.countDown();

            await().atMost(Duration.ofSeconds(5)).until(() -> delivered.size() == 2);

            assertThat(delivered)
                    .as("the first value, then the newest - never the 98 in between")
                    .containsExactly(1, 100);
            assertThat(conflator.submittedCount()).isEqualTo(100);
            assertThat(conflator.coalescedCount()).isEqualTo(98);
        }
    }

    /**
     * The sink is not thread-safe by contract — it writes to WebSocket sessions. A single-threaded
     * executor is what guarantees that, and it must hold even when submissions come from many
     * threads at once.
     */
    @Test
    @DisplayName("the sink is never entered twice at the same time")
    void sinkIsNeverConcurrent() throws Exception {
        AtomicInteger inFlight = new AtomicInteger();
        AtomicInteger overlaps = new AtomicInteger();
        AtomicInteger delivered = new AtomicInteger();

        try (Conflator<Integer> conflator = new Conflator<>("test", value -> {
            if (inFlight.incrementAndGet() > 1) {
                overlaps.incrementAndGet();
            }
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            inFlight.decrementAndGet();
            delivered.incrementAndGet();
        })) {

            CountDownLatch go = new CountDownLatch(1);
            List<Thread> threads = new java.util.ArrayList<>();
            for (int t = 0; t < 8; t++) {
                Thread thread = new Thread(() -> {
                    try {
                        go.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    for (int i = 0; i < 200; i++) {
                        conflator.submit(i);
                    }
                });
                threads.add(thread);
                thread.start();
            }
            go.countDown();
            for (Thread thread : threads) {
                thread.join(TimeUnit.SECONDS.toMillis(20));
            }

            await().atMost(Duration.ofSeconds(5)).until(() -> inFlight.get() == 0);

            assertThat(overlaps).hasValue(0);
            assertThat(delivered.get())
                    .as("1600 submissions, but only as many deliveries as the sink could manage")
                    .isPositive()
                    .isLessThan(1600);
        }
    }

    /** A submission arriving after the last drain must still get its own drain scheduled. */
    @Test
    @DisplayName("a value submitted after the worker went idle is still delivered")
    void nothingIsStrandedAfterIdle() {
        CopyOnWriteArrayList<String> delivered = new CopyOnWriteArrayList<>();

        try (Conflator<String> conflator = new Conflator<>("test", delivered::add)) {
            conflator.submit("first");
            await().atMost(Duration.ofSeconds(5)).until(() -> delivered.contains("first"));

            conflator.submit("second");
            await().atMost(Duration.ofSeconds(5)).until(() -> delivered.contains("second"));

            assertThat(delivered).containsExactly("first", "second");
        }
    }

    @Test
    @DisplayName("a sink that throws does not stop later deliveries")
    void sinkFailureIsNotFatal() {
        CopyOnWriteArrayList<String> delivered = new CopyOnWriteArrayList<>();

        try (Conflator<String> conflator = new Conflator<>("test", value -> {
            if (value.equals("boom")) {
                throw new IllegalStateException("deliberate");
            }
            delivered.add(value);
        })) {
            conflator.submit("boom");
            conflator.submit("fine");

            await().atMost(Duration.ofSeconds(5)).until(() -> delivered.contains("fine"));
        }
    }

    @Test
    @DisplayName("submitting after close is ignored rather than throwing")
    void submitAfterCloseIsSafe() {
        Conflator<String> conflator = new Conflator<>("test", value -> { });
        conflator.close();

        conflator.submit("late");   // must not throw RejectedExecutionException at the caller

        assertThat(conflator.deliveredCount()).isZero();
    }
}
