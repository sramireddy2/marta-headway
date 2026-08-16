package dev.headway.bench;

import dev.headway.bench.Health.RouteHealth;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Seven readers and one writer over four fields that must move together.
 *
 * <p>Reports throughput <em>and</em> torn reads, because either number alone is misleading. The
 * unsynchronised version wins on speed and is wrong; a benchmark that showed only the speed would
 * be an argument for shipping a bug.
 *
 * <p>7:1 understates the real ratio - the dashboard reads state on every HTTP request and every
 * broadcast tick while the stream job writes twice a minute - so anything that looks good here
 * looks better in production, and anything that looks bad here is worse.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(2)
@Warmup(iterations = 5, time = 2)
@Measurement(iterations = 5, time = 2)
@State(Scope.Group)
public class ConsistentReadBench {

    @Param({"plain", "synchronized", "readWriteLock", "stampedOptimistic", "immutableSnapshot"})
    public String impl;

    private RouteHealth health;

    @Setup(Level.Iteration)
    public void setUp() {
        health = switch (impl) {
            case "plain" -> new Health.Plain();
            case "synchronized" -> new Health.Synchronized();
            case "readWriteLock" -> new Health.RwLock();
            case "stampedOptimistic" -> new Health.StampedOptimistic();
            case "immutableSnapshot" -> new Health.ImmutableSnapshot();
            default -> throw new IllegalArgumentException(impl);
        };
        health.update(1);
    }

    /**
     * JMH prints public fields of an {@code @AuxCounters} state alongside the throughput, which is
     * how a correctness result and a performance result end up in the same table.
     */
    @AuxCounters(AuxCounters.Type.EVENTS)
    @State(Scope.Thread)
    public static class Tearing {
        public long tornReads;

        @Setup(Level.Iteration)
        public void reset() {
            tornReads = 0;
        }
    }

    @State(Scope.Thread)
    public static class Writer {
        long seq = 1;
    }

    @Benchmark
    @Group("mixed")
    @GroupThreads(7)
    public long read(Tearing tearing) {
        long observed = health.read();
        if (observed < 0) {
            tearing.tornReads++;
        }
        return observed;
    }

    @Benchmark
    @Group("mixed")
    @GroupThreads(1)
    public void write(Writer writer) {
        health.update(writer.seq++);
    }
}
