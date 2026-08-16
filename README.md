# Headway

**Watches Atlanta's buses in real time and spots when they bunch up.**

If a bus is supposed to come every 10 minutes, you should never wait 25 and then see three arrive
together. But that is exactly what happens, everywhere, constantly. Headway reads MARTA's live bus
feed, works out how far apart the buses actually are, compares that to the timetable, and shows the
problem on a map as it happens.

It is a learning project, built in the open, with every claim measured rather than asserted. When
something surprised me — and a lot did — it is written down here, including the times I was wrong.

---

## Why buses bunch

This is the thing worth understanding, and it takes one minute.

Imagine four buses evenly spaced on a route. Now one of them hits a red light and falls a minute
behind.

1. Running late, it arrives at the next stop to find **more** people waiting — because more time has
   passed since the last bus.
2. More people means longer to board, so it falls **further** behind.
3. Meanwhile the bus behind it finds **fewer** people waiting, boards quickly, and **catches up**.
4. Repeat.

The gap does not recover. It collapses. Within a few stops the two buses are nose to tail, and
behind them is a hole where a bus should have been. Riders at those stops wait three times as long
as the timetable promised, and then watch two buses arrive at once.

This is called **bus bunching**, and it is the single biggest cause of unreliable transit. The
fixes — holding a bus at a stop for a minute, sending it express past a few stops, turning it
around early — only work *while it is happening*. Which means someone has to see it happening.

That is what this project builds.

**"Headway"** is the transit word for the gap between consecutive buses. Ten-minute headway means a
bus every ten minutes.

---

## What you see

A dark map of Atlanta with every bus on it, updating once a second.

- Each dot is a bus, coloured by how well spaced it is: **green** is fine, **orange** and **red**
  mean bunched, **blue** and **purple** mean a hole has opened up, **grey** means we cannot say.
- The two buses in an actual bunching incident get a **white ring**, so you see the specific pair,
  not just a troubled route.
- The side panel lists what is going wrong right now, worst first — *"route 83 Campbellton Road,
  severe bunching, 4 minutes so far, buses 3621 and 5104"* — and clicking a row flies the map to
  those two buses.
- Clicking a bus explains it in plain terms: which route, how it is doing, how far it is from the
  bus in front, and how far it *should* be.

Real example, captured live:

```
Bus 3621
Route         83 — Campbellton Road
Status        on schedule
Spacing       0.57× scheduled
Closest gap   3.5 km of 6.2 km
Seen          28s ago
```

That same bus had been flagged as severely bunched four minutes earlier. You can watch problems
appear, get worse, and recover.

---

## How it works, start to finish

Six stages. Here is the whole thing in plain English first; the details come later.

```
   MARTA's live feed                    every 15 seconds, ~175 buses
          |
          v
   1. Ingest          read it, throw away the repeats, hand it on
          |
          v
   2. Kafka           a durable queue, so nothing is lost if a stage restarts
          |
          v
   3. Spark           group buses by route, measure the gaps between them
          |
          v
   4. Compare         is that gap normal for this route at this time of day?
          |
          v
   5. API             keep the current picture in memory, serve it
          |
          v
   6. Map             draw it in a browser
```

**1. Get the data in.** MARTA publishes where every bus is, as a file that updates roughly every 30
seconds. Headway asks for it every 15 seconds. Half those requests return a file it has already
seen, which is fine and expected — asking more often than the data changes is the only way to get
it promptly, and duplicates are recognised and dropped.

**2. Put it on a queue.** Kafka is a durable log: things get written to it, and readers work through
them at their own pace. It means the part that reads MARTA and the part that does the maths do not
have to run at the same speed, or even at the same time.

**3. Measure the gaps.** This is the hard part, and it is not "how far apart are these two dots".
Two buses 2 km apart in a straight line might be 2 km apart along the road, or 9 km, depending on
how the route winds between them. So each bus's GPS position is snapped onto the route's actual
path and converted into *how far along the route it is*. Once every bus is a single number on a
line, the gap between two buses is just a subtraction.

**4. Decide if it is a problem.** A 400 m gap means nothing by itself. On a route running every 4
minutes it is severe bunching; on one running every 45 minutes it is completely normal. So the
observed gap is compared against what the timetable implies it should be, and the **ratio** is what
gets classified.

**5. Keep the current picture.** A queue is a *history* of measurements. A dashboard needs *the
state right now*. Converting one into the other is a real job, and it is what the API does: it
reads the stream and keeps one up-to-date answer per route in memory.

**6. Draw it.** The browser holds one open connection and receives the complete picture once per
second.

---

## Try it yourself

You need **Java 21** and **Docker Desktop running**. Nothing else — the build tool downloads
itself.

> Java 21 specifically, not 25. Spark does not support 25 yet.

**Build it** (this also runs all 202 tests, and takes a few minutes the first time):

```bash
.\mvnw.cmd clean package
```

**Start the queue:**

```bash
docker compose up -d
```

Wait about 20 seconds, until `docker compose ps` says `healthy`.

**Start reading MARTA** — leave this running in its own terminal:

```bash
.\mvnw.cmd -q -pl headway-ingest -am package exec:java -DskipTests
```

**Start the maths** — another terminal:

```bash
docker compose --profile stream up spark
```

**Start the website** — another terminal:

```bash
java -jar headway-api/target/headway-api-0.1.0-SNAPSHOT.jar
```

**Open <http://localhost:8080/>**

Give it about 90 seconds. The first minute is genuinely empty — the system needs to see each bus
twice before it can measure anything.

**To stop everything:**

```bash
docker compose --profile stream down
```

Then Ctrl+C the other two terminals. On Windows, Ctrl+C does not always kill the Java process
underneath, and a survivor will block your next build:

```powershell
Get-CimInstance Win32_Process -Filter "Name='java.exe'" | Where-Object { $_.CommandLine -like "*headway*" } | Stop-Process -Force
```

### Is it working?

```bash
curl -s http://localhost:8080/api/status
```

`routes` above zero means the whole chain is alive. If `malformedRecords` is climbing, two parts of
the system disagree about the data format — which otherwise looks identical to "no data".

Running it at 3 AM will show almost no buses. That is Atlanta being asleep, not a bug.

---

## The pieces, explained

### Reading the feed

MARTA publishes in **GTFS-Realtime**, the standard format transit agencies use. It arrives as
Protocol Buffers — a compact binary format, not human-readable, roughly 13 KB for the whole fleet.

The interesting problem here is **duplicates**. Every other poll returns a byte-identical file:

```
poll: 186 received | 186 new,   0 updated,   0 stale | 186 vehicles on 65 routes
poll: 186 received |   0 new, 180 updated,   6 stale | 186 vehicles on 65 routes
poll: 186 received |   0 new,   0 updated, 186 stale | 186 vehicles on 65 routes
```

Every reading carries a timestamp, and a reading is only stored if it is **newer** than the one
already held for that bus. Everything else is dropped. This sounds minor and it is load-bearing: it
means re-delivering the same message a hundred times leaves the system in exactly the state one
delivery would. That property is what makes it safe to replay the entire queue from the beginning
after a restart.

**A bug worth describing.** A bus whose GPS transmitter freezes keeps appearing in the feed forever
with the same unchanging timestamp. An early version admitted it (the id was new, so it looked
new), then the cleanup sweep removed it a minute later for being too old, then the next poll added
it again — forever, once a minute. Admission and eviction disagreed about what "too old" meant. Now
one single setting governs both, so the rule holds by construction: *nothing can be let in that the
next sweep would immediately throw out.*

Readings dated more than two minutes in the **future** are also refused. A bus with a wrong clock
would otherwise be permanently stuck: nothing would ever look newer than it, so it could never be
updated, and it would never age out.

### The queue, and why the key matters

Records go into Kafka keyed by **route**. That one choice is worth explaining, because the obvious
alternative is subtly wrong.

Kafka splits a topic into partitions and only guarantees ordering *within* a partition. Records
sharing a key always land in the same partition. Key by **bus id** and the load spreads out
beautifully — and route 15's buses scatter across six partitions, so a reader can process one bus's
10:00:30 reading before another bus's 10:00:15 reading, and compute a gap from two moments that
never coexisted.

Key by **route** and every bus on route 15 stays in one partition, in order. Verified on the
running system: 547 records across 65 routes, spread over all 6 partitions, with **zero routes
split across more than one partition**.

> **The key follows the question you intend to ask, not the load you want to balance.**

### Keeping the pipeline from eating memory

Between reading the feed and writing to Kafka there is a queue with a **hard size limit**. When it
fills, the producer *blocks* — reading stops until the writers catch up.

That sounds like a flaw and it is the entire point. An unbounded queue does not remove a
bottleneck, it hides one: if consumers are slower than producers it grows until memory runs out,
and the failure is the worst kind — minutes of slow degradation, then a crash whose stack trace
does not mention the actual cause. A bounded queue turns that into something harmless. Memory stays
flat and the system runs at the speed of its slowest stage, which is the fastest it could correctly
go anyway.

This is **backpressure**: slowness travelling upstream as a signal instead of piling up as garbage.

You can watch it work by shrinking the queue to 16 slots and feeding it 176-position batches:

```
poll: 176 positions enqueued in 914ms (400ms BLOCKED on a full queue) | queue 0/16
metrics | fetched 702 -> enqueued 702 -> processed 702 | blocked 434ms total | kafka 702 sent / 0 failed
```

**702 in, 702 out**, with a queue eleven times too small to hold a single batch. Nothing dropped.
The producer just waited.

The queue is actually split into four, with a route always going to the same one and each one
drained by exactly one worker. One shared queue with four workers would be faster and would break
the ordering that keying by route exists to protect — two workers could grab consecutive route-15
readings and write them to Kafka in either order.

### The timetable

To know whether a gap is bad, you need to know what it should be. MARTA does not publish a
"buses every N minutes" file, so the timetable has to be reconstructed from `stop_times.txt` —
**2,415,219 rows, 126 MB**, the biggest file in the download.

All that is needed is, per trip, the first departure and the last arrival: two rows out of the
forty-odd each trip contributes. So the file is streamed and reduced as it is read, never held in
memory:

```
Schedule: read 2415218 stop_times rows in 4893ms -> 52401 trips across 169 groups (0 skipped)
```

Scheduled headway is the **median** gap between departures around the current time — median rather
than average, because one mid-morning break between rush hours would drag the average well above
what riders actually experience.

**Two traps this format sets:**

*Times after midnight.* 88,862 rows have an hour of 24 or more, up to 26 — late-night services that
belong to the *previous* day. Java's standard time parser throws an exception on `25:30:00`, so
these are parsed by hand.

*Every kind of day at once.* Weekday, Saturday, Sunday and holiday trips all live in the same file,
separated only by a service id. Skip that filter and a Friday rush hour gets averaged with a Sunday
morning — halving the apparent scheduled headway and making a perfectly spaced fleet look bunched.

### Turning GPS into "how far along the route"

Two buses at opposite ends of a horseshoe-shaped route can be 500 m apart in a straight line and
12 km apart along the road. Straight-line distance is not just imprecise here, it is meaningless.

So each GPS point is snapped onto the route's drawn path and converted into a single number: metres
travelled from the start. Then a gap is a subtraction.

Some care is needed. One degree of longitude in Atlanta is about 92.6 km while one degree of
latitude is about 111.3 km, so treating them as interchangeable stretches every east-west distance
by 20%. Each segment is converted into a local metres grid centred on the point being measured.

**Checked against the shapes themselves:** projecting all 359,676 points of every route back onto
their own route should return each point's own distance. Sideways error came out at **exactly
zero**.

**Then checked against something that shares no code at all.** Self-consistency proves the geometry
is internally correct, not that it means anything real — code that read kilometres as metres would
pass that test perfectly. So the speed implied by *our* numbers (change in computed distance ÷
elapsed time) was compared against the speed the buses' own equipment reports:

```
mean implied speed  (our projection)   : 9.12 m/s
mean reported speed (vehicle hardware) : 9.43 m/s
correlation                            : 0.80
```

**3.3% apart.** And a detail that turned out to matter: MARTA rounds reported speed to exact 5 mph
buckets, so the *reference* measurement carries about ±1.12 m/s of error before our code is
involved. The projection is more accurate than that comparison can show — the comparison can only
put a ceiling on the error.

**The loop problem.** A route that doubles back along the same street has two points on the path
that are equally close to a bus driving there. GPS jitter of a few metres flips which one wins, and
the bus appears to teleport kilometres between updates. The fix is to only search near where the
bus was fifteen seconds ago. That took ambiguous cases from **19 to 4**.

### Measuring gaps as data flows

Apache Spark does the continuous maths. It reads the queue, groups buses by route and direction,
and every 30 seconds looks at a 60-second window of readings.

Three things happen inside a window, in an order that matters:

1. **Keep only the newest reading per bus.** A 60-second window at 15-second polling holds about
   four sightings of the same bus. Skip this and you measure the distance between a bus and
   *itself* moments earlier — tens of metres — and the system reports severe bunching everywhere,
   forever. There is a test named after this.
2. **Sort by position along the route.** Not by bus id, not by arrival order. Position is the only
   ordering in which "the next bus" means anything.
3. **Subtract neighbours.** Those differences are the headways.

**Late data.** A reading stamped 10:00:58 might not arrive until 10:01:04, so a window cannot be
closed the instant the clock passes it. But it cannot stay open forever either, or memory grows
without bound. Spark's answer is a **watermark**: "once you have seen a reading stamped 10:03, treat
every window ending before 10:01 as finished." Two minutes, chosen from measured behaviour — the
feed republishes every ~30 s and vehicle timestamps trail it by up to two minutes. Too short and
real data is silently discarded; too long and results lag. It is a dial between completeness and
latency, not a magic number.

### Deciding what counts as bunching

Observed spacing divided by expected spacing:

| Ratio | Verdict |
|---|---|
| ≤ 0.25 | severe bunching |
| ≤ 0.50 | bunching |
| 0.50 – 1.50 | on schedule |
| ≥ 1.50 | gapping (a hole is opening) |
| ≥ 2.50 | severe gapping |

**Parked buses are not bunched.** An early version kept reporting two buses on route 89 as 38 m
apart. They were sitting at a terminal between runs. A bus is now excluded when it is **stationary
AND near an end of the route** — both conditions, and that is deliberate: "stationary anywhere"
would exclude buses at red lights and busy stops, which are exactly the conditions that *cause*
bunching.

Live output:

```
=== 50 route-direction groups with 2+ buses ===   11 alerts
|140:1 |North Point Parkway   |SEVERE_BUNCHING| 17 m  | 8910 m expected |0.00| every 20 min |[3687, 3681]|
|10:1  |AUC / Hollywood Road  |SEVERE_BUNCHING|2713 m |12503 m expected |0.22| every 30 min |[4663, 4671]|
|71:0  |Cascade Road          |BUNCHING       |3295 m |10290 m expected |0.32| every 20 min |[4658, 4636]|
```

Two buses **17 metres apart** on a route scheduled every 20 minutes. And 11 alerts out of 50 groups
— it discriminates rather than firing on everything, which is the difference between a useful alert
feed and one nobody reads.

### The API, and one event instead of twenty

Sliding windows mean the same three-minute bunching incident gets measured six or more times. Every
one of those measurements is correct. All of them describe **one event**. An alert feed that emits
twenty rows for one problem is an alert feed people learn to ignore.

Spark cannot fix this — a streaming window deliberately has no memory of previous windows, which is
exactly what lets it scale. Fixing it needs a small piece of memory keyed by route, and that is
what the API adds. An **episode** opens the first time a route reports trouble, absorbs every
measurement that agrees, notes if it gets worse, and closes when the route recovers.

Measured live: **17 episodes opened while 70 repeated measurements were absorbed into them.**

Episodes also track worst-so-far separately from right-now, because those answer different
questions. A dispatcher triaging a list wants to know how bad it got; someone watching one route
wants to know if it is recovering. The map shows this as `severe bunching → bunching`.

What is available over HTTP:

| Endpoint | What it gives you |
|---|---|
| `/api/routes` | every route being measured, worst first |
| `/api/alerts` | current problems, as events not measurements |
| `/api/alerts/history` | problems that have since resolved |
| `/api/vehicles` | where every bus is |
| `/api/status` | counters that tell you whether the pipeline is healthy |
| `/ws/live` | a live connection that pushes the whole picture once a second |

### The map

Three files, no framework, no build step. The browser opens one connection and receives a complete
snapshot every second.

**Snapshots, not changes.** Sending only what changed would be smaller, and it would mean a browser
that misses one message is subtly wrong until the next full refresh, and reconnecting needs its own
special path. A complete snapshot has none of those problems: every message stands alone, a dropped
one costs a second, and connecting is the same code as updating. It is affordable because the whole
picture is only about 84 KB.

**Two details that matter more than they look:**

*Move the dots, do not recreate them.* 180 buses redrawn once a second is 10,800 objects a minute if
each update rebuilds them, and it slams shut any popup you are reading mid-sentence. The markers are
kept and moved.

*Back off when reconnecting.* A server restart is exactly when every open browser tab tries to
reconnect at once. Retrying in a tight loop turns one restart into a stampede against a process
that is still starting up.

---

## Things that went wrong

The most useful part of the project. All of these are real, and most were found by running it
rather than by thinking about it.

### The system was reporting bunching everywhere

Because a 60-second window contains four sightings of the same bus, and nobody had said "only keep
the newest one per bus". It was measuring the distance between each bus and itself, fifteen seconds
earlier. Tens of metres. Constant severe bunching, everywhere, permanently.

### Two buses "parked 38 metres apart" on route 89

They were parked. At a depot. Between shifts. The maths was completely right and the answer was
useless — which is a different kind of bug, and a more interesting one.

### Joining on the obvious field silently matched nothing

The live feed says a bus is on `route_id: "15"`. The timetable file also has a column called
`route_id`, and for that same route it contains `26913`. The value `15` lives in
`route_short_name`.

Joining the two `route_id` columns matches **zero rows** — and because it is a left join, nothing
fails. Every route quietly enriches to null and the pipeline keeps running. Verified both ways: 9
of 9 sample buses matched on short name, **0 of 9** matched on route id.

### A field that looks standard and is not

The GTFS spec says `direction_id` is 0 or 1 — outbound or inbound. MARTA's live feed contains 5, 9,
11, 14, 17 and null, and never 0 or 1. Whatever those mean, they are not the standard field.

This matters because headway only makes sense between buses going the *same way*; a northbound and
a southbound bus passing each other are not consecutive in any useful sense. The real direction
comes from the timetable instead, where it is clean: 26,549 trips one way, 25,852 the other,
nothing else.

### The bug the tests caught before it ever ran

The code that filters trips to "services running today" treated an **empty** result as "don't
filter" rather than "nothing runs today". On a Saturday that would have folded every weekday trip
back in, halving the apparent scheduled headway and reporting a perfectly spaced fleet as bunched.
The test failed immediately.

### Spark died and it was the folder's fault

Spark kept crashing with an error claiming two copies of the job were running. There was one. It
also reported corrupted internal state that nothing had corrupted.

Spark's crash-recovery files assume that renaming a file is instantaneous and atomic. This project
lives in a OneDrive folder, and OneDrive syncs files out from under whatever is using them. Moving
those files off the synced folder fixed both errors.

**And then I did something worse:** I fixed it by changing *two* things at once — moving the files
*and* disabling a Spark safety check I had wrongly blamed. That left no way to know which one
worked, while paying a real cost: the check I disabled is the one that catches a corrupted restart.
So I went back and re-ran it with the check switched on. Six clean batches. The check was never the
problem. It is back on, and the wrong diagnosis is recorded in the code as wrong, because a
plausible wrong explanation left in a comment is worse than no comment.

### Every handler returned a 500 error

Java's compiler discards the names of method parameters unless you ask it not to. Spring needs
those names to work out which part of a URL goes into which variable. Spring Boot's standard setup
turns the option on invisibly; this project deliberately does not use that setup, so it inherited
the requirement without the fix. Every single web request failed until the flag was added.

### A field was missing from the output and the tests were happy

The API was supposed to send `durationSeconds` — how long a problem had been going on. It never
appeared. The test passed because it parsed the response back into an object and *recalculated* the
duration, so it never noticed the field was absent from the actual data.

Found by reading real output. The test now checks the raw text.

### A dependency I never asked for broke a module that never used it

Adding Spring Boot to the project pinned a networking library to an older version — across the
*whole* build. Spark, in a completely separate module that has never heard of Spring, immediately
crashed on startup.

> **A shared dependency list is not a suggestion for the module that wants it. It is a constraint on
> everything that inherits it.**

### The number I got wrong

I claimed 93% of incoming readings were duplicates, reasoning from "11,833 records covering 177
buses". That conflated *"we have seen this bus before"* with *"this reading is a duplicate"* —
buses genuinely do move between readings.

Measured properly, it is **51%**: 1,565 stale out of 3,090 processed. Still enough to justify the
optimisation built on it, but the number I first published was wrong and is corrected everywhere it
appeared.

---

## Measuring the concurrency claims

Lots of projects assert things about thread safety in comments. This one measures them, with JMH —
the standard Java benchmarking tool, which runs code in a separate process and handles the ways
naive timing loops lie to you.

All figures on an 8-core Intel Core Ultra 7, throughput in operations per microsecond, **higher is
better**.

### Updating shared state

The core operation: many threads, one shared map, "store this reading only if it is newer".

| Approach | 1 thread | 8 threads |
|---|---:|---:|
| Unsafe version (**loses data**) | 23.1 | 84.7 |
| **What this project uses** | 21.6 | **66.0** |
| Standard atomic update | 22.2 | 54.6 |
| `StampedLock` | 28.5 | 29.9 |
| `synchronized` | **32.4** | 14.5 |
| Read/write lock | 15.4 | 5.1 |

**`synchronized` is the fastest option on one thread and gets *slower* with eight.** 32.4 down to
14.5, while the atomic version goes *up* to 54.6. A single-threaded benchmark would have confidently
recommended the thing that collapses under load.

**A read/write lock is the worst correct option at every thread count.** Taking a *read* lock is
still a write to the lock's own bookkeeping, so readers fight each other over one piece of memory.

**The unsafe version really is faster.** "Correct code costs nothing" would have been a convenient
result and it is false. The honest argument is that the difference buys you a silently lost update —
which an earlier test reproduced at 5 runs in 40.

### The optimisation this found

The atomic update takes a lock even when it decides to change nothing — and about half of all
readings are duplicates that change nothing. So a cheap lock-free check now runs first, and returns
early only when it can **prove** the reading is not newer. **7.5× faster** on that path.

This is not the unsafe version wearing a disguise, and the distinction is exact: the unsafe
version's decision is *final*, this one can only skip work. Stored timestamps never move backwards,
so "not newer than what I just read" stays true no matter what another thread does next. Anything
else falls through to the locked path that makes the real decision.

### Reading several fields that must agree

Seven readers, one writer, four values that must come from the same update.

| Approach | reads | wrong answers |
|---|---:|---:|
| No synchronisation | 672.9 | **6,972,619,214** |
| `StampedLock` optimistic read | 498.4 | 0 |
| Immutable snapshot | 449.3 | 0 |
| `synchronized` | 16.1 | 0 |
| Read/write lock | 5.8 | 0 |

The unsynchronised version is the fastest by a distance and mismatched its own fields **seven
billion times**. A benchmark reporting only the speed column would be an argument for shipping a
bug.

### Two techniques deliberately not used

The plan was to add `StampedLock` and Guava's `Striped` locks. The measurements said don't, and
following the measurement is the point of taking it.

**`StampedLock`** is genuinely impressive — 86× a read/write lock. But an immutable object behind a
single reference is within 10% on reads and **twice as fast on writes**, because a reader takes one
value and then holds something nobody can change. That is already the pattern used throughout this
project. Adding a lock to match what immutability gives for free would be a downgrade.

**Striped locks** work — 3.3× a single global lock — but only when heavily over-provisioned. With 8
stripes across 8 threads they are statistically indistinguishable from one global lock. *"Use
striping"* is not the advice; *"use far more stripes than you have threads"* is. And restructuring
the data to avoid locking entirely beat every locking variant.

Both are implemented in the benchmark module, where they document the trade-off without imposing
it.

Run them yourself (takes about 20 minutes):

```bash
java -jar headway-bench/target/benchmarks.jar
```

---

## Project layout

```
headway-common     the shared data types and JSON rules
headway-gtfs       the timetable: routes, trips, route shapes, the geometry
headway-ingest     reads MARTA, decodes it, publishes to Kafka
headway-stream     the Spark job that measures gaps
headway-api        the website and its live data feed
headway-bench      benchmarks for the concurrency claims above
```

`headway-gtfs` deliberately knows nothing about live vehicles, and `headway-common` deliberately
knows nothing about Kafka. The Spark module and the web module are kept apart because Spark and
Spring Boot each drag in large, opinionated, conflicting sets of dependencies — as the Netty
incident above demonstrated.

## Configuration

Everything has a working default. The ones worth knowing:

| Variable | Default | What it does |
|---|---|---|
| `HEADWAY_KAFKA_BOOTSTRAP` | `localhost:9092` | Where the queue is |
| `HEADWAY_QUEUE_CAPACITY` | `256` | Slots per queue shard — set it to `4` to watch backpressure engage |
| `HEADWAY_STARTING_OFFSETS` | `latest` | `earliest` fills the dashboard instantly from stored history |

## Troubleshooting

**The map is blank for the first minute.** Expected. The system needs to see each bus at least
twice before it can measure a gap.

**`Failed to delete ...headway-common-0.1.0-SNAPSHOT.jar`** — a Java process from a previous run is
still alive and holding the file. Closing the terminal does not always kill it. Use the
`Stop-Process` command in the shutdown section above.

**`JAVA_HOME environment variable is not defined correctly`** — either it is not set, or the
terminal was opened before it was set. Note that it must point at the JDK *folder*, not at
`bin\java.exe`.

**`Timed out talking to Kafka at localhost:9092`** — the queue is not up. `docker compose up -d`,
then wait for `docker compose ps` to say healthy.

**Editing the map's files while the site is running does nothing.** They are served from the built
copy, not from the source folder. Rebuild and restart. Hard-refreshing the browser will not help —
the server really is sending the old version.

## Data sources

| Feed | Format |
|---|---|
| [Live vehicle positions](https://gtfs-rt.itsmarta.com/TMGTFSRealTimeWebService/vehicle/vehiclepositions.pb) | GTFS-Realtime protobuf |
| [Scheduled timetable](https://itsmarta.com/google_transit_feed/google_transit.zip) | GTFS static (zipped CSV) |

No API key needed. Be polite — the poller enforces a hard ceiling of one request per 15 seconds
regardless of what the rest of the code asks for.

Map tiles from [CARTO](https://carto.com/attributions), map data from
[OpenStreetMap](https://www.openstreetmap.org/copyright) contributors.

## License

MIT. MARTA's data is published under their own terms.
