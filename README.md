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
| 2 | Domain model, scheduled polling, rate limiting, idempotent dedupe | ☐ |
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

Check your setup:

```bash
java -version
```

## Build and run

Run the tests:

```bash
mvnw -B test
```

Fetch the live MARTA feed and print what's out there right now:

```bash
mvnw -q -pl headway-ingest -am package exec:java -DskipTests
```

Expected output (numbers vary — this is a live feed):

```
Fetching https://gtfs-rt.itsmarta.com/TMGTFSRealTimeWebService/vehicle/vehiclepositions.pb
Fetched 14848 bytes
GTFS-RT version 2.0 | feed timestamp 2026-08-14T21:03:39Z (4s old)
188 entities in feed -> 188 usable vehicle positions
65 routes currently have vehicles reporting. Busiest 10:
   route 121 -> 7 vehicles
   route 15 -> 7 vehicles
   ...
   route 1 | vehicle 2380 | (33.78998, -84.40430) | 13.4 m/s | age 5s
```

Runs late at night will show far fewer vehicles. Zero vehicles is normal around 2–4 AM.

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
