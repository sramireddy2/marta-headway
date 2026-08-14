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
| 3 | Kafka in Docker + keyed producer | ☐ |
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
- **Docker Desktop** (from step 3 onward, for Kafka).
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

> **PowerShell users:** PowerShell will not run a script from the current directory without a
> leading `.\`, and it needs the `.cmd` extension. Use `.\mvnw.cmd`. In Git Bash, macOS, or Linux
> use `./mvnw` instead.

Run the tests:

```bash
.\mvnw.cmd -B test
```

Poll the live MARTA feed continuously and hold every bus in memory. Ctrl+C to stop:

```bash
.\mvnw.cmd -q -pl headway-ingest -am package exec:java -DskipTests
```

Expected output (numbers vary — this is a live feed):

```
Starting ingest: poll every 15s, ceiling 0.0667 req/s, evict after 10min
poll: 193 received | 193 new,   0 updated,   0 stale | 193 vehicles on 66 routes | 718ms
poll: 193 received |   0 new,   0 updated, 193 stale | 193 vehicles on 66 routes |  33ms
poll: 192 received |   0 new, 186 updated,   6 stale | 193 vehicles on 66 routes |  27ms
poll: 192 received |   0 new,   0 updated, 192 stale | 193 vehicles on 66 routes |  28ms
Evicted 5 stale vehicles; 188 remain
poll: 191 received |   4 new, 184 updated,   3 stale | 192 vehicles on 66 routes |  37ms
```

Reading that: the first poll is all new. Then **every other poll is 100% stale** — MARTA
republishes roughly every 30 seconds, so a 15-second poll fetches the byte-identical file half the
time. The idempotency check absorbs the whole duplicate batch and the store does not move.

That is not a bug to tune away. Polling faster than the publish rate is deliberate: you do not know
the publisher's phase, so the only way to see a new file promptly is to ask more often than it
changes. The correctness property that makes it safe — a re-delivered message is a no-op — is the
same one that will let you replay a Kafka topic from the beginning in step 3 without corrupting
anything.

The `Evicted 5` line is a separate thread sweeping out buses that finished their shift.

Runs late at night will show far fewer vehicles. Zero vehicles is normal around 2–4 AM.

## Concurrency notes

The interesting parts, and where to read them:

| Concern | Where | Approach |
|---------|-------|----------|
| Shared mutable state | [`VehicleStore`](headway-ingest/src/main/java/dev/headway/ingest/VehicleStore.java) | `ConcurrentHashMap.compute` — read-decide-write as one atomic step, avoiding a check-then-act race |
| Idempotency / late data | `VehiclePosition.isSupersededBy` | Per-vehicle last-seen timestamp; older readings dropped |
| Consistent reads | `VehicleStore.snapshot()` | Guava `ImmutableMap` copy, so readers never see a half-applied batch |
| Contended counters | `VehicleStore` | `LongAdder`, not `AtomicLong` |
| Unbounded growth | `VehicleStore.evictStale` | Vehicles silent for 10 minutes are swept |
| Politeness to MARTA | [`FeedPoller`](headway-ingest/src/main/java/dev/headway/ingest/FeedPoller.java) | Guava `RateLimiter` as a hard ceiling, independent of the scheduler's cadence |
| Scheduler survival | `FeedPoller.run()` | Catches `Throwable`; an escaping exception silently cancels a `scheduleWithFixedDelay` task forever |
| Overload behaviour | [`IngestService`](headway-ingest/src/main/java/dev/headway/ingest/IngestService.java) | `scheduleWithFixedDelay`, not `AtFixedRate`, so slow responses never cause a thundering catch-up |
| Clean shutdown | `IngestService.close()` | `shutdown` → `awaitTermination` → `shutdownNow` |

The concurrency test in `VehicleStoreTest` is not decorative: the same workload run against a
naive `get()`-then-`put()` implementation served a stale position on 5 of 40 runs.

## Module layout

```
headway-parent          the root pom: dependency versions, Java level, module list
├── headway-common      domain model shared by everything (VehiclePosition)
└── headway-ingest      polls GTFS-Realtime, decodes protobuf, emits domain objects
```

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
