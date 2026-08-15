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
| 5 | Static GTFS (routes, trips, shapes) + Guava LoadingCache | ✅ |
| 6 | Project GPS onto route shape → "distance along route" | ✅ |
| 7 | Spark Structured Streaming headway computation | ✅ |
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

## The scheduled feed

`headway-gtfs` downloads `google_transit.zip` and parses the three files the headway calculation
needs. Check coverage against the live feed:

```bash
.\mvnw.cmd -q -pl headway-ingest -am package exec:java -DskipTests "-Dexec.mainClass=dev.headway.ingest.GtfsCoverageMain"
```

```
Loaded static GTFS in 1229ms: 86 routes, 52401 trips, 215 shapes, published 2026-06-24
Live feed: 176 vehicles
RESOLVED 176 of 176 vehicles (100.0%)
  no trip_id in the realtime feed : 0
  trip_id not in trips.txt        : 0
  route short name not in routes  : 0
  direction split: {direction 0=82, direction 1=94}
   2309 -> route 2 (Donald Lee Hollowell/Ponce de Leon) | dir 0 | Candler Park Stn | shape 136095, 8.6 km
```

**100% of live vehicles resolve to a route, a direction and a path.** That was the open question
from step 3, and it is the precondition for step 6 — a vehicle that cannot be resolved has no
direction and no shape, so it cannot take part in a headway calculation at all.

### Joining realtime to scheduled: two ids, two different answers

| Realtime field | Joins to | Verified |
|---|---|---|
| `trip_id` | `trips.trip_id` — direct match | 6/6 sampled ids found |
| `route_id` | `routes.route_short_name`, **not** `routes.route_id` | 9/9 matched short name, **0/9** matched route_id |

A realtime bus on Clifton Road reports `route_id: "15"`. In `routes.txt` that row's `route_id` is
`26913` and its `route_short_name` is `15`. Joining on `route_id` matches nothing — and as a left
join it fails *silently*, enriching every route to null while the pipeline keeps running. Hence
`GtfsSnapshot.routeForRealtimeId(...)`, named so the mistake is hard to make.

### direction_id, resolved

The realtime feed's `direction_id` is unusable (5, 9, 11, 14, 17, null — no 0 or 1). The static
feed's is correct: **26,549 trips with direction 0 and 25,852 with direction 1**, nothing else. So
direction comes from `trips.txt` via `trip_id`, and `TripContext.headwayGroup()` returns
`route:direction` — the key that groups buses which can meaningfully bunch with each other.

### Data facts worth knowing

- `shape_dist_traveled` is in **kilometres**. Verified by summing haversine along shape 136092:
  10,815 m against a final value of 10.8389, a ratio of 997.8 m per unit. Converted to metres at
  parse time so one unit exists everywhere else.
- All 359,676 shape points across 215 shapes are present, in sequence, and monotonic — but the
  loader validates rather than assumes, and falls back to computing haversine distances for feeds
  that leave the column blank.
- `frequencies.txt` is **absent**, so scheduled headway in step 8 has to be derived from
  `stop_times.txt` rather than read off directly.

### Two performance decisions

**`ZipFile`, not `ZipInputStream`.** Uncompressed the archive is ~147 MB, and 126 MB of that is
`stop_times.txt`, which is not needed until step 8. `ZipInputStream` would decompress every entry
in order to reach the ones we want; `ZipFile` reads the central directory and opens only the three
we need, so `stop_times.txt` is never decompressed at all.

**Shapes as three `double[]`, not a `List<Point>`.** 359,676 points as objects means 359,676 heap
allocations reached through pointers. Three parallel primitive arrays hold the same data in ~8.6 MB
of contiguous memory, which is what step 6's per-vehicle nearest-segment scan will walk.

## Projecting GPS onto the route

Two buses at `(33.75, -84.39)` and `(33.77, -84.41)` are 2.7 km apart as the crow flies. That
number is useless — buses follow streets, and the route between them might be 2.8 km or 9 km
depending on how it winds. `ShapeProjector` snaps each GPS point onto the route's polyline and
returns **how far along the route it is**, turning two coordinates into two positions on a line.
On a line, the gap between two buses is a subtraction.

```bash
.\mvnw.cmd -q -pl headway-ingest -am package exec:java -DskipTests "-Dexec.mainClass=dev.headway.ingest.HeadwayPreviewMain"
```

### Why not planar geometry on degrees

At Atlanta's latitude one degree of longitude is ~92.6 km while one degree of latitude is
~111.3 km. Treating them as equal stretches every east-west distance by 20% and picks the wrong
segment near diagonal corners. Each segment is instead converted into a local east-north plane in
metres, centred on the query point:

```
x = (lon - queryLon) * 111195.08 * cos(queryLat)
y = (lat - queryLat) * 111195.08
```

That is the equirectangular approximation — sub-metre accurate over the tens of km a route spans,
and two multiplications instead of trigonometry per segment. Centring on the query point also puts
it at the origin, which simplifies the point-to-segment maths.

Distance along the route interpolates the feed's own `shape_dist_traveled` rather than summing
computed segment lengths, so projection error never accumulates along the route.

### Validated against 359,676 real shape points

Projecting a shape's own vertices back onto that shape must return each vertex's own distance:

```
9093 vertices across 215 shapes in 498ms (18259 projections/sec)
worst cross-track error   : 0.0000 m   <- the geometry itself
worst along-route error   : 13458.3 m (shape 136624)
vertices with >50 m along-route error: 19 of 9093 (0.21%), across 16 shapes
  still wrong when given a previous-position hint: 4
```

**Cross-track error is exactly zero** — the geometry is right. The 13.4 km along-route error with
*zero* cross-track is not a bug: shape 136624 passes through that identical coordinate twice. The
question "where on the route is this?" genuinely has two answers.

### The loop problem, and the fix

A route that loops or doubles back along one street has two segments near-equally close to any
point on the doubled section. GPS jitter of a few metres flips which one wins, and the bus appears
to teleport kilometres between polls.

`projectNear(shape, lat, lon, hintMetres, windowMetres)` searches only the stretch near the
vehicle's *previous* position. A bus that was at 8,000 m fifteen seconds ago is still within a few
hundred metres of that, so the far-away duplicate is excluded outright. It is also faster — a
binary search over the sorted cumulative distances, then a scan of a fraction of the polyline.

Measured effect: **19 ambiguous vertices → 4**. The residue is tight doublings-back where both
candidates fall inside the plausible-movement window. Affected shapes are 16 of 215 (7%), and
affected positions 0.21%.

### Live GPS quality

```
164 vehicles projected in 11.3ms (69us per vehicle)
cross-track: median 5.1 m | p90 15.6 m | p99 2832.5 m | max 5443.6 m
8 of 164 vehicles are more than 100 m off route
```

Median 5 m and p90 16 m means real fixes land essentially on the shape. The handful of multi-km
outliers are buses assigned to a trip they have not started — deadheading to the route's start.
`ShapeProjection.isOnRoute(maxCrossTrackMetres)` exists for exactly this; step 7 should discard
projections beyond roughly 150 m rather than compute headways from them.

### Accuracy: checked against something that shares no code

Self-consistency proves the geometry is internally correct, but not that the numbers mean anything
about the real world — a projector using the wrong distance units would still pass it. So
`ProjectionAccuracyMain` compares two independent measurements:

- **Implied speed** — the change in *our* computed distance-along-route between two sightings,
  divided by elapsed time. Entirely our geometry and the shape file.
- **Reported speed** — what the bus's own equipment put in the feed. Never touched by our code.

```bash
.\mvnw.cmd -q -pl headway-ingest -am package exec:java -DskipTests "-Dexec.mainClass=dev.headway.ingest.ProjectionAccuracyMain"
```

Two samples 75 seconds apart, 154 vehicles tracked across both:

```
implied speed over 35 m/s (physically absurd)  : 0 (0.0%)
appeared to move backwards by more than 20 m   : 4 (2.6%)
implied speed: median 5.30 m/s | p90 13.56 m/s | max 21.43 m/s

mean implied speed  (our projection)   : 9.12 m/s
mean reported speed (vehicle hardware) : 9.43 m/s
correlation                            : 0.80
absolute difference: median 2.05 m/s | p90 4.62 m/s
```

The two means agree to **3.3%**, with zero physically impossible speeds. A projector reading
kilometres as metres, or picking wrong segments, would fail this immediately and obviously.

**How to read the 2.05 m/s median difference.** MARTA quantizes reported speed to exact 5 mph
buckets — the only values in the feed are 0.44704, 2.2352, 4.4704, 6.7056, 8.9408, 11.176,
13.4112, 15.6464, 17.8816, 22.352 and 26.8224 m/s, which are 1, 5, 10 … 60 mph exactly. The
reference measurement therefore carries ±1.12 m/s of quantization error before our code is
involved, and about 43% of vehicles report no speed at all. The projection's real error is smaller
than the number above; the comparison can only bound it.

**The 2.6% that appear to move backwards** are the honest residue — loop ambiguity and shape
mismatches. They show up as one bad sample rather than persistent drift, so step 7's windowing
should absorb them, but they are real and not yet zero.

### Actual gaps, right now

```
group 121:1 — 4 buses on a 28.8 km shape
   4625 at  6171.3 m
   3670 at  9106.2 m   gap  2934.9 m
   5117 at 24686.0 m   gap 15579.8 m
   4605 at 28680.3 m   gap  3994.3 m

tightest gaps anywhere in the system:
    19.0 m apart on group 140:1
    92.7 m apart on group 89:0
```

Two buses **19 metres apart** on route 140. That is bunching, live, detected end to end — and it is
what step 7 turns into an alert.

### Caching: `refreshAfterWrite` vs `expireAfterWrite`

The zip is cached on disk and re-fetched with a **conditional GET** — `If-Modified-Since` against
MARTA's `Last-Modified`. A `304` costs a few hundred bytes instead of 21 MB, and unlike a
once-a-day timer it stays correct whether the agency publishes twice in a day or not at all for a
month.

In memory it sits in a single-entry Guava `LoadingCache`, with **both** deadlines set:

- **`expireAfterWrite(24h)`** invalidates the entry, so the next caller *blocks* while it reloads —
  here that means a 21 MB download plus a 360,000-point parse, seconds of stall.
- **`refreshAfterWrite(6h)`** keeps serving the existing snapshot and reloads in the background.
  Nobody blocks; one thread does the work.

Refresh at 6h is the normal path; expire at 24h is the backstop so repeated refresh failures
eventually surface instead of serving a week-old schedule forever.

One subtlety: Guava's default `CacheLoader.reload()` is **synchronous** — it just calls `load()` on
the triggering thread, so plain `refreshAfterWrite` still blocks somebody. Making it genuinely
async requires overriding `reload()` to return a `ListenableFuture`, which
`GtfsStaticRepository` does. A failed refresh returns the previous snapshot rather than throwing,
so a MARTA outage degrades to "slightly stale", never to "no schedule".

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

## The Spark streaming job

```
Kafka vehicle-positions
     |  parse JSON against a declared schema
     v
project onto the route shape        (broadcast GTFS + step 6's projector)
     |  drop anything >150 m off route
     v
watermark 2 min, window 60s / slide 30s, group by route:direction
     |  newest reading per vehicle, order by distance, difference neighbours
     v
console  +  Kafka route-headways
```

Build the submit jar, then run it (Kafka must already be up):

```bash
.\mvnw.cmd -q -pl headway-stream -am package -DskipTests
```

```bash
docker compose --profile stream up spark
```

### Why Spark runs in Docker and not on the host

Not a preference — a requirement on Windows. Structured Streaming **checkpointing** goes through
Hadoop's filesystem layer, which calls native `winutils.exe` / `hadoop.dll` that Windows does not
ship:

```
java.io.FileNotFoundException: HADOOP_HOME and hadoop.home.dir are unset
```

Batch Spark works fine on Windows — a `spark.range(1,11).count()` smoke test passed on Java 21
with no flags at all — which is exactly why this only appears once you add a checkpoint. The usual
workaround is downloading unofficial `winutils` binaries; running the official `apache/spark` image
is cleaner, reproducible, and the reason the broker advertises two listeners.

The container reaches Kafka at **`kafka:29092`** (the `DOCKER` listener), while the ingest service
on the host uses `localhost:9092`. That two-listener config from step 3 is what makes both work at
once.

### Watermarking

A window covering 10:00:00–10:01:00 cannot be closed the moment the clock passes 10:01 — a
vehicle's 10:00:58 reading may arrive at 10:01:04. But Spark cannot wait forever either, or state
grows without bound. `withWatermark("ts", "2 minutes")` says: once an event stamped 10:03 arrives,
consider every window ending before 10:01 closed and drop its state.

Two minutes is chosen from measured behaviour, not taste: MARTA republishes every ~30 s, vehicle
timestamps trail the feed timestamp by up to ~2 minutes, and step 5's audit found one bus 708 s
stale. Too low and real data is silently dropped; too high and memory grows and results lag. It is
a completeness-against-latency dial.

**Sliding, not tumbling.** 60 s windows every 30 s means each instant is covered twice, so a
bunching event straddling a boundary cannot be split and missed by both windows.

### Details worth knowing

| Decision | Reason |
|---|---|
| Declared JSON schema | Inferring a schema from a stream samples whatever arrives first, and can differ between restarts — which invalidates the checkpoint |
| Broadcast the GTFS snapshot | 8.6 MB of shapes; a captured reference would serialise a copy into *every task*. Broadcast ships it once per executor |
| One `foreachBatch`, not two `writeStream`s | Two queries read the source topic twice; and `orderBy` needs `Complete` mode, which retains every window forever and defeats the watermark. A batch DataFrame sorts freely |
| `batch.persist()` inside `foreachBatch` | The batch is consumed twice (show + Kafka write); without it Spark reruns the whole aggregation, projection UDF included |
| `spark.sql.shuffle.partitions=8` | The default 200 means 200 near-empty tasks per batch for ~65 groups — the most common local-Spark slowdown |
| Dedupe to newest-per-vehicle | A 60 s window at 15 s polling holds ~4 sightings of the same bus; without deduping you measure the gap between a bus and itself and report constant bunching |
| `provided` Spark, shaded Kafka connector | The image supplies Spark; the Kafka connector is not in it. Excluding Hadoop and Scala from the shade took the jar from 72 MB to 18.8 MB |

### It works

```
=== batch 5: 134 route-direction groups with 2+ buses ===
|windowEnd          |headwayGroup|routeLongName        |vehicleCount|minGapMetres|maxGapMetres|
|2026-08-15 02:30:30|95:1        |Metropolitan Parkway |2           |25.6        |25.6        |
|2026-08-15 02:30:00|89:0        |Old National Highway |4           |38.1        |10095.3     |
|2026-08-15 02:30:30|1:1         |Joseph E. Lowery Blvd|3           |1804.6      |4585.8      |
```

269 records landed on `route-headways`, each carrying the ordered vehicles and distances so an
alert can name *which* two buses are too close:

```json
{"headwayGroup":"83:1","routeLongName":"Campbellton Road","vehicleCount":2,
 "minGapMetres":4216.7,"orderedVehicles":["3541","3531"],
 "orderedDistancesMetres":[79.5,4296.2]}
```

Only batch 0 fell behind the 30 s trigger (36 s — JIT warmup plus broadcasting the shapes);
batches 1–5 kept up.

### Tests

The headway arithmetic — deduplicate, order, difference — lives in `HeadwayGaps`, a plain Java
class with no Spark in it. That separation is the point: a bug there silently produces wrong
headways, and leaving it inside a UDF would mean the only way to test it was to stand up a
`SparkSession`, which is slow enough that in practice it does not get tested at all.

- **`HeadwayGapsTest`** — 20 tests, 0.3 s, no Spark. The load-bearing one is
  `oneBusIsNotAConvoy`: four sightings of a single bus in one window must produce **zero** gaps.
  Skip the dedupe and you measure the distance between a bus and itself moments earlier, and every
  route reports severe bunching forever.
- **`HeadwayFunctionsSparkTest`** — 6 tests through a real DataFrame, covering what the pure tests
  cannot: Catalyst `Row` conversion, broadcasting the schedule, the declared JSON schema against a
  message copied from the live topic, and the sharp edge where an array column arrives as a
  `scala.collection.Seq` rather than a `List` (a runtime `ClassCastException`, never a compile
  error). Batch, not streaming, since checkpointing is exactly what does not work on Windows.

These caught a real latent bug immediately: pinning `scala-library` to 2.13.16 when Spark 4.1.3 is
built against 2.13.17 throws `NoSuchMethodError: MurmurHash3$.caseClassHash` the moment a
`SparkSession` is created. Scala's standard library is not binary compatible across patch releases
in the way the version number suggests.

### A finding that step 8 has to handle

Route 89 direction 0 reported a 38.1 m gap in every window. Pulling the full record:

```
89:0  vehicles=[3503, 3694, 3519, 3507]
      distances: [0.0, 38.1, 10133.4, 13165.8]
```

Vehicles 3503 and 3694 sat at 0.0 m and 38.1 m, unchanged across three windows spanning 90
seconds. They are **parked at the terminal**, not bunched — and the exact `0.0` is step 6's
clamping, meaning 3503 is at or before the route start.

Two buses on layover at a depot are not an operational problem, and a dispatcher alerted about
them would stop reading the alerts. **Step 8 must exclude stationary vehicles at route endpoints**,
or every terminal becomes a permanent false alarm.

## Troubleshooting

**`Failed to clean project: Failed to delete ...headway-common-0.1.0-SNAPSHOT.jar`**
A previous run's JVM is still alive and holding the jar. Stopping the terminal does not always stop
the Java process it launched. Find and kill it:

```bash
Get-CimInstance Win32_Process -Filter "Name='java.exe'" | Where-Object { $_.CommandLine -like "*headway*" } | ForEach-Object { Stop-Process -Id $_.ProcessId -Force }
```

**`The JAVA_HOME environment variable is not defined correctly`**
Either `JAVA_HOME` is wrong, or the terminal predates the fix — see [Set JAVA_HOME](#set-java_home).

**`Timed out talking to Kafka at localhost:9092`**
The broker is not up. `docker compose up -d`, then wait for `docker compose ps` to say healthy.

## Module layout

```
headway-parent          the root pom: dependency versions, Java level, module list
├── headway-common      domain model + the JSON contract (VehiclePosition, Json)
├── headway-gtfs        the scheduled feed: routes, trips, shapes, projection geometry
├── headway-ingest      polls GTFS-Realtime, decodes protobuf, publishes to Kafka
└── headway-stream      Spark job: Kafka -> windowed headways -> Kafka
```

`headway-gtfs` deliberately does **not** depend on `headway-common`: it knows about the scheduled
feed and nothing about realtime vehicle positions, so it can be tested and reused on its own. The
two worlds meet in `headway-ingest`, which depends on both.

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

The static zip is 21 MB (≈147 MB uncompressed) and is cached under `data/`, which is gitignored.

No API key required. Be polite: poll no faster than every 15 seconds (step 2 enforces this with a
rate limiter).

## License

MIT
