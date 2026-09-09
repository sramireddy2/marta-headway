<div align="center">

# 🚌 Headway

**Watches Atlanta's buses in real time and spots when they bunch up.**

If a bus is scheduled every 10 minutes, you shouldn't wait 25 just to watch three pull up together. Headway taps into MARTA's live feed to measure the actual gaps between buses, compares them against the timetable, and instantly paints the problem on a map as it unfolds.

<br/>

![Java](https://img.shields.io/badge/Java_21-ED8B00?style=for-the-badge&logo=openjdk&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Kafka_4-231F20?style=for-the-badge&logo=apachekafka&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Spark_4.1-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Spring Boot](https://img.shields.io/badge/Spring_Boot_3-6DB33F?style=for-the-badge&logo=springboot&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![Leaflet](https://img.shields.io/badge/Leaflet-199900?style=for-the-badge&logo=leaflet&logoColor=white)

<br/>

![Live map](https://img.shields.io/badge/live_map-once_a_second-0ea5e9?style=flat-square)
![Tests](https://img.shields.io/badge/tests-202-22c55e?style=flat-square)
![License](https://img.shields.io/badge/license-MIT-a855f7?style=flat-square)
![No API key](https://img.shields.io/badge/MARTA_feed-no_API_key-f59e0b?style=flat-square)

</div>

A learning project. Every claim below was measured on the live MARTA feed, including the times I was wrong.

---

## Tech stack

Each piece is there because a real constraint showed up, not because a tutorial listed it.

| Layer | Tools | What it does |
| :--- | :--- | :--- |
| **Language** | ![Java](https://img.shields.io/badge/Java_21-ED8B00?style=flat-square&logo=openjdk&logoColor=white) Maven wrapper | One JDK. Spark does not support Java 25 yet. |
| **Live feed** | ![GTFS](https://img.shields.io/badge/GTFS--Realtime-005DAA?style=flat-square) Protocol Buffers | ~175 buses, polled every 15 seconds. |
| **Timetable** | ![GTFS](https://img.shields.io/badge/GTFS_static-0F9D58?style=flat-square) Commons CSV | Reconstructs scheduled headway from 2.4 million `stop_times` rows. |
| **Queue** | ![Kafka](https://img.shields.io/badge/Kafka_4.3-231F20?style=flat-square&logo=apachekafka&logoColor=white) KRaft | Durable log keyed **by route**, so buses on the same line stay in order. |
| **Stream** | ![Spark](https://img.shields.io/badge/Spark_4.1-E25A1C?style=flat-square&logo=apachespark&logoColor=white) | Snap GPS onto the route, measure gaps, classify bunching vs gapping. |
| **API** | ![Spring](https://img.shields.io/badge/Spring_Boot_3.5-6DB33F?style=flat-square&logo=springboot&logoColor=white) REST + WebSocket | Turns a history of measurements into "what is true right now." |
| **Map** | ![Leaflet](https://img.shields.io/badge/Leaflet_1.9-199900?style=flat-square&logo=leaflet&logoColor=white) vanilla JS | Three static files. No framework, no bundler. |
| **Run / proof** | ![Docker](https://img.shields.io/badge/Docker-2496ED?style=flat-square&logo=docker&logoColor=white) ![JUnit](https://img.shields.io/badge/JUnit_5-25A162?style=flat-square&logo=junit5&logoColor=white) JMH | Kafka + Spark in Compose. 202 tests plus concurrency benchmarks. |

```
MARTA live feed  →  Kafka (by route)  →  Spark (gaps)  →  Spring Boot  →  Leaflet map
  every ~15s         6 partitions         every 30s       1 snapshot/s     1 Hz dots
```

---

## Why buses bunch

One bus hits a red light and falls a minute behind. It then finds **more** people waiting, boards slower, and falls further behind. The bus behind finds **fewer** people, boards fast, and catches up. Within a few stops they are nose to tail, and behind them is a hole where a bus should have been.

That is **bus bunching**. The fixes only work *while it is happening*, so someone has to see it. **"Headway"** is the gap between consecutive buses — ten-minute headway means a bus every ten minutes.

---

## What you see

A dark map of Atlanta, every bus, updating once a second.

<p>
  <img alt="on schedule" src="https://img.shields.io/badge/on_schedule-22c55e?style=for-the-badge" />
  <img alt="bunched" src="https://img.shields.io/badge/bunched-f97316?style=for-the-badge" />
  <img alt="severe bunching" src="https://img.shields.io/badge/severe_bunching-ef4444?style=for-the-badge" />
  <img alt="gapping" src="https://img.shields.io/badge/a_hole_opened-3b82f6?style=for-the-badge" />
  <img alt="severe gapping" src="https://img.shields.io/badge/severe_gapping-a855f7?style=for-the-badge" />
  <img alt="unknown" src="https://img.shields.io/badge/cannot_say-6b7280?style=for-the-badge" />
</p>

Dots are buses. A **white ring** marks the pair in an actual incident. The side panel lists problems worst-first; click a row to fly to those two buses, or click a bus for route, spacing, and how far it *should* be.

---

## How it works

1. **Ingest** — poll MARTA's GTFS-Realtime protobuf every 15s. About half the polls are duplicates; only a *newer* timestamp replaces what we already have.
2. **Queue** — Kafka keyed by **route**, not bus id. Ordering is per partition, and headway only makes sense among buses on the same line.
3. **Measure** — snap each GPS point onto the route shape (straight-line distance is meaningless on a winding road). Gap = subtraction. Compare the ratio to the timetable: ≤0.5 is bunching, ≥1.5 is a hole.
4. **API** — Spark windows report the same incident many times. The API collapses those into one **episode** that opens, absorbs, and closes.
5. **Map** — one WebSocket, a full snapshot every second (~84 KB). Markers are moved, not rebuilt.

Parked buses at a terminal are excluded (stationary **and** near a route end). Red lights still count — that is how bunching starts.

---

## Run it

**Java 21** (not 25) and **Docker Desktop**. The Maven wrapper downloads itself.

```bash
.\mvnw.cmd clean package          # also runs 202 tests
docker compose up -d              # wait until `docker compose ps` says healthy
```

Then three terminals:

```bash
.\mvnw.cmd -q -pl headway-ingest -am package exec:java -DskipTests
docker compose --profile stream up spark
java -jar headway-api/target/headway-api-0.1.0-SNAPSHOT.jar
```

Open [http://localhost:8080/](http://localhost:8080/). The first minute is empty — each bus has to be seen twice. `curl -s http://localhost:8080/api/status` should show `routes` above zero.

```bash
docker compose --profile stream down
```

Ctrl+C the other terminals. On Windows a leftover Java process will lock jars:

```powershell
Get-CimInstance Win32_Process -Filter "Name='java.exe'" | Where-Object { $_.CommandLine -like "*headway*" } | Stop-Process -Force
```

3 AM looks empty. That is Atlanta asleep, not a bug.

---

## Layout

```
headway-common     shared types + JSON
headway-gtfs       timetable, shapes, geometry
headway-ingest     MARTA → Kafka
headway-stream     Spark gap maths
headway-api        REST, WebSocket, map
headway-bench      JMH concurrency numbers
```

Spark and Spring Boot live in separate modules because their dependency trees fight. Kafka: `HEADWAY_KAFKA_BOOTSTRAP` (default `localhost:9092`).

---

## Things that actually happened

- Measuring a bus against **itself** 15s earlier reported severe bunching everywhere. Keep the newest reading per bus in the window.
- Two buses "38 m apart" on route 89 were parked at a depot between shifts.
- Live `route_id` `"15"` does not match GTFS `route_id` `26913`. Join on `route_short_name` or you match **zero** rows and nothing fails.
- MARTA's live `direction_id` is 5, 9, 11… never the GTFS 0/1. Real direction comes from the timetable.
- Spark crashed because checkpoints lived on a OneDrive bind mount. Non-atomic renames. Move them off the synced folder.
- Spring Boot in the parent POM downgraded Netty for Spark. A shared BOM is a constraint on every module, not a suggestion.
- I published "93% of pings are duplicates." Measured: **51%**. Corrected everywhere it appeared.

JMH on an 8-core Ultra 7: `synchronized` is fastest single-threaded and **slows down** at 8 threads; the lock-free-then-`compute` path used here goes the other way. Run `java -jar headway-bench/target/benchmarks.jar` (~20 min).

---

## Feeds

| Feed | Format |
|---|---|
| [Live vehicle positions](https://gtfs-rt.itsmarta.com/TMGTFSRealTimeWebService/vehicle/vehiclepositions.pb) | GTFS-Realtime protobuf |
| [Scheduled timetable](https://itsmarta.com/google_transit_feed/google_transit.zip) | GTFS static zip |

No API key. The poller will not request faster than once per 15 seconds.

Map tiles: [CARTO](https://carto.com/attributions). Map data: [OpenStreetMap](https://www.openstreetmap.org/copyright).

## License

MIT. MARTA's data is under their own terms.
