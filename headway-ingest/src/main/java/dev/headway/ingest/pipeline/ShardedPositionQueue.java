package dev.headway.ingest.pipeline;

import dev.headway.common.VehiclePosition;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * The bounded hand-off between the threads that fetch and the threads that process.
 *
 * <h2>Why bounded</h2>
 *
 * An unbounded queue does not remove a bottleneck, it hides one. If consumers are slower than
 * producers, an unbounded queue grows until the heap is gone — and the failure mode is the worst
 * available: minutes of rising latency and GC thrash, then {@code OutOfMemoryError}, with the
 * actual cause (a slow consumer) nowhere in the stack trace.
 *
 * <p>A bounded queue converts that into something benign. When it fills, {@link #put} <b>blocks</b>
 * the producer. The producer stops fetching. Memory stays flat. The system runs at exactly the
 * speed of its slowest stage, which is the fastest it could correctly go anyway.
 *
 * <p>That is <b>backpressure</b>: the consumer's slowness propagating upstream as a signal instead
 * of accumulating as garbage. The bound is not a tuning knob you set high to be safe — a bound of
 * one million is an unbounded queue with extra steps. It should be sized so that blocking happens
 * before memory becomes interesting.
 *
 * <h2>Why sharded, and not one queue</h2>
 *
 * One queue with N workers would give parallelism and quietly break step 3. Two workers pulling
 * consecutive route-15 readings can call {@code producer.send()} in either order, so records reach
 * the partition out of sequence — destroying the exact ordering guarantee that keying by route was
 * chosen to provide.
 *
 * <p>So the queue is split into shards, a route is assigned to a shard by hashing its id, and
 * <b>each shard is drained by exactly one worker</b>. Route 15 is always shard 3, always handled by
 * worker 3, always published in arrival order. Parallelism across routes, strict order within one.
 *
 * <p>This is the same idea as the Kafka partitioning it protects — and the same idea as Guava's
 * {@code Striped} locks in step 11. Hash the key, take the lane, keep the lane to yourself.
 *
 * <p>The trade-off is honest: an unusually busy route makes its shard the slow one, and no other
 * worker can help. With ~65 routes over 4 shards that is a rounding error, and correctness is not
 * something to trade for it.
 */
public final class ShardedPositionQueue {

    private final List<BlockingQueue<VehiclePosition>> shards;
    private final int shardCount;
    private final int capacityPerShard;
    private final IngestMetrics metrics;

    public ShardedPositionQueue(int shardCount, int capacityPerShard, IngestMetrics metrics) {
        if (shardCount < 1) {
            throw new IllegalArgumentException("shardCount must be >= 1");
        }
        if (capacityPerShard < 1) {
            throw new IllegalArgumentException("capacityPerShard must be >= 1");
        }
        this.shardCount = shardCount;
        this.capacityPerShard = capacityPerShard;
        this.metrics = metrics;

        List<BlockingQueue<VehiclePosition>> built = new ArrayList<>(shardCount);
        for (int i = 0; i < shardCount; i++) {
            // ArrayBlockingQueue, not LinkedBlockingQueue: the capacity is fixed at construction
            // and the backing array is allocated once, so there is no per-element node allocation
            // and no way to accidentally create an unbounded one by omitting an argument.
            built.add(new ArrayBlockingQueue<>(capacityPerShard));
        }
        this.shards = List.copyOf(built);
    }

    /**
     * Which shard owns a route.
     *
     * <p>{@code Math.floorMod} rather than {@code %}: {@code hashCode()} can be negative, and
     * {@code -7 % 4} is {@code -3} in Java, which would index out of bounds. This is a real bug
     * that appears only for keys that happen to hash negative, so it survives casual testing.
     */
    public int shardFor(String routeId) {
        return Math.floorMod(routeId.hashCode(), shardCount);
    }

    /**
     * Enqueues a position, blocking if its shard is full.
     *
     * <p>Blocking is the feature. The alternative designs are both worse for this system:
     * {@code offer()} without waiting silently drops data, and an unbounded queue trades a clean
     * stall for an eventual crash.
     *
     * @return nanoseconds spent blocked — zero when there was room, which is the normal case
     */
    public long put(VehiclePosition position) throws InterruptedException {
        BlockingQueue<VehiclePosition> shard = shards.get(shardFor(position.routeId()));

        // Fast path: don't pay for timing when there is room, which is almost always.
        if (shard.offer(position)) {
            metrics.recordEnqueued();
            return 0L;
        }

        long start = System.nanoTime();
        shard.put(position); // blocks here until a worker takes something
        long blockedNanos = System.nanoTime() - start;

        metrics.enqueueWait().record(blockedNanos, TimeUnit.NANOSECONDS);
        metrics.recordEnqueued();
        return blockedNanos;
    }

    /**
     * Takes the next position from one shard, waiting up to {@code timeout}.
     *
     * <p>Called only by that shard's single worker. Returns {@code null} on timeout so the worker
     * can re-check whether it has been asked to stop, rather than blocking forever on a queue that
     * will never receive anything again.
     */
    public VehiclePosition poll(int shard, long timeout, TimeUnit unit) throws InterruptedException {
        return shards.get(shard).poll(timeout, unit);
    }

    /** Total items waiting across every shard. */
    public int depth() {
        int total = 0;
        for (BlockingQueue<VehiclePosition> shard : shards) {
            total += shard.size();
        }
        return total;
    }

    public int depthOf(int shard) {
        return shards.get(shard).size();
    }

    public boolean isEmpty() {
        return depth() == 0;
    }

    /** Fraction full, 0.0 to 1.0. The number to alert on long before it reaches 1. */
    public double utilization() {
        return (double) depth() / totalCapacity();
    }

    public int shardCount() {
        return shardCount;
    }

    public int capacityPerShard() {
        return capacityPerShard;
    }

    public int totalCapacity() {
        return shardCount * capacityPerShard;
    }

    /** Per-shard depths, for spotting one hot route starving the rest. */
    public List<Integer> depths() {
        List<Integer> out = new ArrayList<>(shardCount);
        for (BlockingQueue<VehiclePosition> shard : shards) {
            out.add(shard.size());
        }
        return List.copyOf(out);
    }
}
