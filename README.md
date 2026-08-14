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
| 4 | Bounded queue, backpressure, virtual threads, Micrometer metrics | ☐ |
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
| Clean shutdown | `IngestService.close()` | `shutdown` → `awaitTermination` → `shutdownNow` |

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
