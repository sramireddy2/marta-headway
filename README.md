# Headway

**A real-time bus bunching detector for MARTA (Atlanta).**

Buses on a route are supposed to be evenly spaced. If a bus comes every 10 minutes, the gap — the
**headway** — between consecutive buses should be 10 minutes. In practice they clump:

- One bus runs slightly late.
- Being late, it finds more passengers waiting at each stop, so it dwells longer and falls further behind.
- The bus behind it finds *fewer* passengers, so it speeds up and closes the gap.
- The feedback loop runs away. Eventually three buses arrive nose-to-tail, followed by a 30-minute hole.

This is **bus bunching**, and it is the single largest cause of unreliable transit. Agencies want to
see it *live*, because the fixes — holding a bus at a stop, short-turning it, expressing it past
stops — only work in the moment.

Headway ingests MARTA's live GTFS-Realtime feed, computes the actual headway between consecutive
buses on each route, flags bunching and gapping as it happens, and serves it over a REST/WebSocket
API and a live map.

---

## Architecture (target)

```
MARTA GTFS-Realtime  ──poll──▶  Ingest service  ──▶  Kafka  ──▶  Spark Structured   ──▶  Kafka
(protobuf over HTTP)            (Java 21,            topic:      Streaming              topic:
 every ~15-30s                   virtual threads,    vehicle-    (window + watermark,    headway-
                                 bounded queue,      positions    project onto route      alerts
                                 rate limiter)       keyed by     shape, sort, diff)         │
                                                     route_id                                │
                                                                                             ▼
                                        Browser  ◀──WebSocket──  Spring Boot API  ◀──consume──
                                     (Leaflet map)   + REST       (Jackson)
```

## Status

| Step | What it adds | Done |
|------|--------------|:----:|
| 1 | Repo skeleton, Maven build, GTFS-Realtime protobuf decoding | ✅ |
| 2 | Scheduled polling, rate limiting, idempotent concurrent store | ✅ |
| 3 | Kafka in Docker + producer keyed by route | ✅ |
| 4 | Bounded sharded queue, backpressure, virtual threads, Micrometer | ✅ |
| 5 | Static GTFS ingest (routes, trips, shapes) + Guava LoadingCache | ☐ |
| 6 | Project GPS onto route shape → "distance along route" | ☐ |
| 7 | Spark Structured Streaming headway computation | ☐ |
| 8 | Bunching / gapping detection + alert topic | ☐ |
| 9 | Spring Boot REST + WebSocket API | ☐ |
| 10 | Leaflet live map front-end | ☐ |
| 11 | Concurrency hardening (StampedLock / Striped locks) + benchmarks | ☐ |

---

## Requirements

- **JDK 21.** Not 25 — Apache Spark supports 17 and 21 only, and step 7 depends on it.
- **Docker Desktop**, running. Kafka lives in Compose from step 3 onward.
- Nothing else. Maven is supplied by the wrapper (`mvnw` / `mvnw.cmd`), which downloads itself.

### Set JAVA_HOME

The Maven wrapper reads `JAVA_HOME`, and it must point at the JDK **folder** — not at
`java.exe`, and not at a path that no longer exists. Check it:

```bash
echo $env:JAVA_HOME
```

If it is wrong, set it once (PowerShell), then **open a new terminal** — environment changes only
apply to shells started afterwards:

```bash
[Environment]::SetEnvironmentVariable("JAVA_HOME","C:\Program Files\Eclipse Adoptium\jdk-21.0.9.10-hotspot","User")
```

## Build and run

### Start Kafka

```bash
docker compose up -d
```

Wait until it reports healthy (about 20 seconds):

```bash
docker compose ps
```

`docker compose down` stops it and keeps the data; `docker compose down -v` wipes the log too.

> **PowerShell users:** PowerShell will not run a script from the current directory without a
> leading `.\`, and it needs the `.cmd` extension. Use `.\mvnw.cmd`. In Git Bash, macOS, or Linux
> use `./mvnw` instead.

Run the tests:

```bash
.\mvnw.cmd -B test
```

Poll the live MARTA feed continuously, hold every bus in memory, and publish each position to
Kafka. Ctrl+C to stop:

```bash
.\mvnw.cmd -q -pl headway-ingest -am package exec:java -DskipTests
```

Configuration comes from the environment, with working defaults:

| Variable | Default | Meaning |
|----------|---------|---------|
| `HEADWAY_KAFKA_BOOTSTRAP` | `localhost:9092` | Broker address |
| `HEADWAY_KAFKA_TOPIC` | `vehicle-positions` | Destination topic |

Expected output (numbers vary — this is a live feed):

```
Starting ingest: poll every 15s, ceiling 0.0667 req/s, evict after 10min
poll: 186 received | 186 new,   0 updated,   0 stale, 0 rejected | 186 vehicles on 65 routes | 1122ms
poll: 186 received |   0 new, 180 updated,   6 stale, 0 rejected | 186 vehicles on 65 routes |   47ms
poll: 186 received |   0 new,   0 updated, 186 stale, 0 rejected | 186 vehicles on 65 routes |   22ms
poll: 186 received |   0 new, 181 updated,   5 stale, 0 rejected | 186 vehicles on 65 routes |   20ms
poll: 186 received |   0 new,   0 updated, 186 stale, 0 rejected | 186 vehicles on 65 routes |   23ms
```

| Column | Meaning |
|--------|---------|
| `received` | Vehicles in the file MARTA just served |
| `new` | A vehicle id not currently in the store |
| `updated` | A strictly newer reading replaced the stored one |
| `stale` | Duplicate or out-of-order — dropped, and entirely normal |
| `rejected` | Refused at the door: already older than the freshness window, or future-dated |

**Every other poll is 100% stale.** MARTA republishes roughly every 30 seconds, so a 15-second poll
fetches the byte-identical file half the time. The idempotency check absorbs the whole duplicate
batch and the store does not move.

That is not a bug to tune away. Polling faster than the publish rate is deliberate: you do not know
the publisher's phase, so the only way to see a new file promptly is to ask more often than it
changes. The correctness property that makes it safe — a re-delivered message is a no-op — is the
same one that will let you replay a Kafka topic from the beginning in step 3 without corrupting
anything.

### Freshness: one threshold, both directions

A vehicle whose GPS transponder freezes stays listed in the feed forever with an unchanging
timestamp. An earlier version admitted such a vehicle (its id was not in the store, so it looked
"new"), and the eviction sweep then removed it a minute later, and the next poll re-added it —
once a minute, indefinitely. Admission and eviction disagreed about what "too old" meant.

`VehicleStore` now takes a single `maxAge` and uses it for **both**, so the invariant holds by
construction: *nothing can be admitted that the next sweep would immediately remove.* Readings
more than two minutes in the future are refused for a related reason — nothing would ever look
newer than them, and they would never age past the cutoff, so they would be permanently stuck.

Runs late at night will show far fewer vehicles. Zero vehicles is normal around 2–4 AM.

## Kafka

Topic `vehicle-positions`, 6 partitions, **keyed by `routeId`**, values as JSON.

```
Partition:0  15  {"vehicleId":"2322","routeId":"15","tripId":"10785433","directionId":5,
                  "latitude":33.79291915893555,"longitude":-84.3209228515625,
                  "bearingDegrees":null,"speedMetersPerSecond":null,
                  "timestamp":"2026-08-14T22:54:08Z"}
```

### Why the key is the route

Kafka guarantees ordering **within a partition** and promises nothing across partitions. A keyed
record is hashed to a partition, so every record sharing a key stays in one partition, in order.

Headway compares buses on the same route against each other. Spread route 15 across six partitions
and a consumer can read 10:00:30 for one bus before 10:00:15 for the bus ahead of it, then compute
a gap from two readings that never coexisted. Keying by route makes that impossible.

Keying by `vehicleId` is the tempting mistake — it balances load more evenly, and it destroys
exactly the ordering the calculation needs. **The key follows the query you intend to run, not the
load distribution.**

Verified against the running broker: 547 records over 65 routes, spread across all 6 partitions,
with **zero routes split across more than one partition**.

```bash
docker exec headway-kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic vehicle-positions
```

### Producer settings that matter

| Setting | Value | Why |
|---------|-------|-----|
| `acks` | `all` | A leader can acknowledge and die before any follower has the record. With one local replica this is free; it is already correct when it stops being free. |
| `enable.idempotence` | `true` | A retry after a lost acknowledgement would otherwise duplicate the record. The broker dedupes by sequence number — the same property `VehicleStore` enforces in memory, now enforced on the wire. |
| `linger.ms` | `20` | ~190 records per poll leave as a few requests instead of 190 round trips. |
| `max.block.ms` | `10000` | The default 60s means a dead broker looks like a hang rather than an error. |

`send()` is asynchronous — it buffers and returns. Calling `.get()` on the returned future per
record makes it synchronous and collapses throughput; the publisher uses a callback instead.

## The ingest pipeline

```
 scheduler          fetch pool           bounded sharded queue          workers
 1 platform thread  virtual threads      4 shards x 256 slots           4 platform threads
      |                   |                       |                          |
  every 15s -------> submit fetch ------> put() blocks when full ----> store + Kafka
                     (blocking HTTP)      route -> shard by hash       1 worker per shard
```

### Backpressure: why the queue is bounded

An unbounded queue does not remove a bottleneck, it hides one. If consumers are slower than
producers it grows until the heap is gone, and the failure mode is the worst available: minutes of
rising latency and GC thrash, then `OutOfMemoryError`, with the real cause — a slow consumer —
nowhere in the stack trace.

A bounded queue turns that into something benign. When it fills, `put()` **blocks the producer**.
Fetching stops. Memory stays flat. The system runs at the speed of its slowest stage, which is the
fastest it could correctly go anyway.

That is backpressure: slowness propagating upstream as a *signal* instead of accumulating as
garbage. Note that a bound of one million is an unbounded queue with extra steps — the bound has to
be small enough that blocking happens before memory gets interesting.

`ShardedPositionQueueTest.backpressureBlocksTheProducer` proves both halves against a deliberately
slow consumer: the producer records real blocked time, **and** the queue never exceeds its bound.
The second assertion is the one that matters — without the bound the test would pass faster and the
queue would have grown to 40 items, which is the start of the curve that ends in an OOM.

#### Seeing it happen live

At the default 1024 slots against ~190 positions per poll the queue never fills, so the mechanism
never engages. Shrink it to prove it works:

```bash
$env:HEADWAY_QUEUE_CAPACITY = "4"
```

16 total slots against 175-position batches. Real output:

```
Starting ingest: ... 4 shards x 4 = 16 queue slots
poll: 176 positions enqueued in 914ms (400ms BLOCKED on a full queue) | queue 0/16
poll: 176 positions enqueued in  32ms ( 12ms BLOCKED on a full queue) | queue 6/16
metrics | fetched 702 -> enqueued 702 -> processed 702 | queue 0/16 0% | blocked 434ms total
        | kafka 702 sent / 0 failed
```

**fetched 702 → enqueued 702 → processed 702 → 702 published, with a queue eleven times too small
to hold one batch.** Nothing was dropped and memory never moved; the producer simply waited. Unset
the variable to go back to the normal configuration.

| Variable | Default | Purpose |
|----------|---------|---------|
| `HEADWAY_QUEUE_SHARDS` | `4` | Shards, and therefore workers |
| `HEADWAY_QUEUE_CAPACITY` | `256` | Slots per shard |

### Why sharded, and not one queue

One queue with four workers would give parallelism and quietly break step 3. Two workers pulling
consecutive route-15 readings can call `producer.send()` in either order, so records reach the
partition out of sequence — destroying the ordering that keying by route exists to provide.

So the queue is split into shards, a route is assigned to one by hashing its id, and **each shard is
drained by exactly one worker**. Route 15 is always shard 3, always worker 3, always published in
arrival order. Parallel across routes, strictly ordered within one — the same idea as the Kafka
partitioning it protects, and the same idea as Guava's `Striped` locks in step 11.

The trade-off is real: an unusually busy route makes its shard the slow one and no other worker can
help. With ~65 routes over 4 shards that is a rounding error, and correctness is not worth trading
for it.

### Three thread types, chosen separately

| Stage | Threads | Why |
|-------|---------|-----|
| Scheduler | 1 platform | Java 21 has no virtual-thread scheduled executor and this thread only submits. Keeping the timer off the work pool stops a slow fetch delaying the next tick. |
| Fetches | Virtual | A fetch is almost all socket wait. A platform thread parked on I/O costs ~1 MB of stack and an OS scheduler slot; a virtual thread costs a few hundred bytes because the JVM unmounts it while it waits. |
| Workers | 4 platform, fixed | Workers do CPU work on in-memory data. Virtual threads make *blocking* cheap, not computation faster, and one per task would create unbounded concurrency over bounded CPU. The count is fixed by ordering anyway — one per shard. |

**Virtual threads for waiting, platform threads for working.** With a single feed today the virtual
thread benefit is latent, not measured; the point is that adding the trip-updates feed in step 5 and
other agencies later costs nothing.

### Shutdown order

Scheduler → fetch pool → workers (which drain their shard first) → then the caller flushes Kafka.
Reversing any two loses data: stop workers first and the queue is abandoned; flush Kafka first and
the last records are produced after the flush.

## Metrics

Micrometer, currently against an in-memory `SimpleMeterRegistry` logged every 30 seconds. Step 9
swaps in Spring Boot's Prometheus registry and the same meters appear over HTTP with no call-site
changes.

The two that matter are `headway.queue.utilization` and `headway.enqueue.wait`. A pipeline that is
coping and one that is a single slow consumer away from stalling look identical from outside —
same log lines, same throughput — right up until they are not. The difference lives entirely in how
full the queue is and how long producers spend blocked.

## Known data quirks

**MARTA's `direction_id` is not the GTFS direction.** The spec says 0 or 1 (outbound/inbound). The
live feed yields 5, 9, 11, 14, 17 and null — no 0 or 1 at all.

This is not cosmetic. Headway only means something between buses travelling the *same way*; a
northbound and a southbound bus passing each other are not consecutive, and treating them as such
invents bunching that is not happening. Real direction has to come from joining `tripId` to the
static GTFS `trips.txt`, which step 5 loads. Until then, nothing branches on this field.

## Concurrency notes

The interesting parts, and where to read them:

| Concern | Where | Approach |
|---------|-------|----------|
| Shared mutable state | [`VehicleStore`](headway-ingest/src/main/java/dev/headway/ingest/VehicleStore.java) | `ConcurrentHashMap.compute` — read-decide-write as one atomic step, avoiding a check-then-act race |
| Idempotency / late data | `VehiclePosition.isSupersededBy` | Per-vehicle last-seen timestamp; older readings dropped |
| Consistent reads | `VehicleStore.snapshot()` | Guava `ImmutableMap` copy, so readers never see a half-applied batch |
| Contended counters | `VehicleStore` | `LongAdder`, not `AtomicLong` |
| Unbounded growth | `VehicleStore.evictStale` | Vehicles silent for 10 minutes are swept |
| Admission / eviction agreement | `VehicleStore.isAdmissible` | One `maxAge` governs both, so a reading can never be accepted and then immediately swept |
| Politeness to MARTA | [`FeedPoller`](headway-ingest/src/main/java/dev/headway/ingest/FeedPoller.java) | Guava `RateLimiter` as a hard ceiling, independent of the scheduler's cadence |
| Scheduler survival | `FeedPoller.run()` | Catches `Throwable`; an escaping exception silently cancels a `scheduleWithFixedDelay` task forever |
| Overload behaviour | [`IngestService`](headway-ingest/src/main/java/dev/headway/ingest/IngestService.java) | `scheduleWithFixedDelay`, not `AtFixedRate`, so slow responses never cause a thundering catch-up |
| Clean shutdown | `IngestService.close()` | Ordered: scheduler → fetches → workers (draining) → Kafka flush |
| Memory safety under load | [`ShardedPositionQueue`](headway-ingest/src/main/java/dev/headway/ingest/pipeline/ShardedPositionQueue.java) | Bounded `ArrayBlockingQueue`; `put()` blocks instead of growing |
| Ordering vs. parallelism | `ShardedPositionQueue.shardFor` | Route hashed to a shard, one worker per shard — parallel across routes, ordered within one |
| Negative hash indexes | `ShardedPositionQueue.shardFor` | `Math.floorMod`, not `%` or `Math.abs` — `abs(Integer.MIN_VALUE)` is still negative |
| Cheap blocking I/O | [`IngestService`](headway-ingest/src/main/java/dev/headway/ingest/IngestService.java) | Virtual threads for fetches, platform threads for CPU-bound workers |
| Observability | [`IngestMetrics`](headway-ingest/src/main/java/dev/headway/ingest/pipeline/IngestMetrics.java) | Micrometer; queue depth and blocked time make backpressure visible |

The concurrency test in `VehicleStoreTest` is not decorative: the same workload run against a
naive `get()`-then-`put()` implementation served a stale position on 5 of 40 runs.

## Module layout

```
headway-parent          the root pom: dependency versions, Java level, module list
├── headway-common      domain model + the JSON contract (VehiclePosition, Json)
└── headway-ingest      polls GTFS-Realtime, decodes protobuf, publishes to Kafka
```

`headway-common` deliberately has **no** Kafka dependency. The domain model defines what a vehicle
position *is* and how it is written as JSON; how those bytes get transported is the ingest module's
concern, and Spark in step 7 will read the same JSON without going through Kafka's serializer API
at all.

More modules arrive with later steps (`headway-stream` for Spark, `headway-api` for Spring Boot).
They are kept separate on purpose: Spark and Spring Boot both drag in large, opinionated,
*conflicting* dependency trees (notably different Jackson versions). Separate modules means
separate classpaths and no version war.

## Data sources

| Feed | URL | Format |
|------|-----|--------|
| Vehicle positions (realtime) | `https://gtfs-rt.itsmarta.com/TMGTFSRealTimeWebService/vehicle/vehiclepositions.pb` | GTFS-RT protobuf |
| Trip updates (realtime) | `https://gtfs-rt.itsmarta.com/TMGTFSRealTimeWebService/tripupdate/tripupdates.pb` | GTFS-RT protobuf |
| Static schedule | `https://www.itsmarta.com/google_transit_feed/google_transit.zip` | GTFS (zip of CSVs) |

No API key required. Be polite: poll no faster than every 15 seconds (step 2 enforces this with a
rate limiter).

## License

MIT
