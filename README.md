# Replicated Log Stream Engine

A distributed, replicated commit log with automatic leader failover, paired with a parallel stream-processing layer for windowed aggregation — built from scratch in Java to demonstrate **distributed systems design**, **concurrency & synchronization**, and **big data stream processing**.

This is a scaled-down but functionally honest version of the ideas behind systems like Kafka (partitioned, replicated log) and Spark Streaming / Flink (windowed stream aggregation), implemented end-to-end rather than just used as a library.

---

## What it does

- Splits a stream of events into **partitions**, each owned by a **broker** process, with consistent key-based routing (same key always lands on the same partition).
- Replicates every partition from a **leader** to a **follower** broker **synchronously** — a write is only acknowledged once it exists on two machines.
- Detects a dead leader automatically via **heartbeats**, and **promotes the follower** to leader with no manual intervention.
- Producers detect the failover, **redirect writes** to the newly promoted leader, and retry with backoff through the brief promotion window.
- Guarantees **idempotent writes** via per-producer sequence numbers, so retries during failover never produce duplicate records.
- Parallel **consumer worker threads** (one per partition) feed a shared windowed aggregator computing count, average, and max temperature per device per 10-second window.
- A **WindowFlusher** scheduler periodically emits completed window results and handles late-arriving events within a configurable grace period.

---

## Proof

### Benchmark results (4 partitions, 500 devices, 15s runs)

| Target       | Actual       | p50 | p95 | p99 | Lost |
|--------------|--------------|-----|-----|-----|------|
| 1,000 ev/s   | 999 ev/s     | 0ms | 0ms | 1ms | 0    |
| 5,000 ev/s   | 5,000 ev/s   | 0ms | 0ms | 0ms | 0    |
| 10,000 ev/s  | 10,000 ev/s  | 0ms | 0ms | 0ms | 0    |
| 20,000 ev/s  | 11,330 ev/s  | 0ms | 0ms | 0ms | 0    |

Hits target exactly up to 10,000 ev/s. Saturates at ~11,300 ev/s on a single machine — bottleneck at that point is the single-threaded accept loop per broker. Zero data loss at every load level.

> **Optimization note:** initial benchmark showed only 511 ev/s at a 1,000 ev/s target. Profiling identified the bottleneck as a new TCP socket per request. Fixed with persistent connections and `TCP_NODELAY`, achieving a 20× throughput improvement.

### Failover under load
25,999 events produced at 2,000 ev/s across 2 partitions.

Leader for partition 0 killed at t=5s.

Result: 25,999 consumed, 0 lost, 0 write failures.

Aggregation continued producing correct windowed results

through the failure with no gap in output.

### Automated regression test (FailoverIntegrationTest)
200 writes attempted. Leader killed at write 80.

198 succeeded, 2 transient failures during ~1s promotion window.

Zero data loss. Zero duplicates. Zero ordering violations.

Runs in ~4 seconds on every mvn test.

---

## Why this design (and not full Raft)

A production-grade consensus protocol like Raft has a large, easy-to-get-subtly-wrong edge case surface (term numbers, split votes, conflicting log truncation) that typically takes experienced engineers weeks to implement correctly. This project instead uses a **simplified primary-backup replication scheme**: one leader and one follower per partition, synchronous replication, heartbeat-based failure detection, and follower self-promotion on leader death.

This preserves every concept that matters for the distributed-systems story — replication, consistency under writes, fault tolerance, automatic failover — while staying achievable and, more importantly, debuggable within the project's timeframe. It's a deliberate, explainable engineering tradeoff, not a shortcut.

---

## Architecture
             ┌──────────────┐      synchronous        ┌──────────────┐
Producer ───► │   Leader     │ ──── replication ──────► │   Follower   │

(key-based     │   Broker     │      (per write)         │   Broker     │

routing)      │ Partition 0  │                          │ Partition 0  │

└──────┬───────┘                          └──────┬───────┘

│                                         │

│         heartbeats (1s interval)        │

└────────────────◄────────────────────────┘

FailoverCoordinator watches leader,

promotes follower if heartbeats stop
Follower ───► ConsumerWorker ───► StreamAggregator ───► WindowFlusher

(has all                           (ConcurrentHashMap      (emits completed

replicated                         .merge() per key)       10s windows)

data)

| Component | Responsibility | Key skill |
|---|---|---|
| `PartitionLog` | Thread-safe, append-only, offset-ordered record store | Concurrency |
| `BrokerServer` | Owns partitions; leader or follower role; TCP server | Distributed systems |
| Replication | Synchronous push of entries to follower before ack | Distributed systems |
| `HeartbeatMonitor` | Detects dead peers via missed heartbeats | Distributed systems |
| `FailoverCoordinator` | Promotes follower to leader on detected failure | Distributed systems |
| `Producer` | Key-based routing; detects NOT_LEADER; retries with backoff | Distributed systems |
| Idempotency (`appendIdempotent`) | Per-producer sequence numbers prevent duplicate writes | Correctness |
| `ConsumerWorker` | Polls one partition, deserializes events, feeds aggregator | Big data |
| `StreamAggregator` | Tumbling-window count/avg/max per device, thread-safe | Big data + Concurrency |
| `WindowFlusher` | Periodic flush of completed windows, late-event handling | Big data |
| `BenchmarkRunner` | Rate-limited load generator, throughput + latency measurement | Big data |

---

## Concurrency design notes

These are the decisions worth explaining in an interview:

**`PartitionLog` uses a single `ReentrantLock`** guarding both offset assignment and the backing list, for both reads and writes. Reads are locked deliberately — `ArrayList` is not safe to read while another thread is resizing it during a concurrent append.

**`WindowAggregate` is immutable.** `combine()` returns a new instance rather than mutating the existing one. This is required for correctness with `ConcurrentHashMap.merge()`: `merge()` is atomic at the map level but does not protect mutations inside the remapping function. A mutable `combine()` causes lost updates under concurrent access — exactly the bug our stress test caught (8,000 expected, 7,979 actual) before the fix.

**`AtomicLong` for shared counters.** `totalEventsProcessed++` on a plain `long` is a non-atomic read-modify-write that loses increments under contention. Caught by the same stress test.

**`ConcurrentHashMap`** is used for all shared broker state (partition ownership, leader flags, heartbeat timestamps) — chosen for guaranteed cross-thread visibility, not just thread-safe mutation.

**`volatile`** is used for simple single-flag state (`running`, `alreadyPromoted`) read by one thread and written by another, where a full lock would be unnecessary overhead.

**`ScheduledExecutorService`** is used for both heartbeat ticking and window flushing rather than hand-rolled `while(true)+sleep()` loops — it handles scheduling drift, survives unexpected exceptions in a tick, and shuts down cleanly.

**Stress tests prove it.** `PartitionLogTest` releases 10 threads simultaneously via `CountDownLatch` to maximise lock contention and asserts no duplicate or missing offsets. `StreamAggregatorTest` hammers the same `(device, window)` key from 8 threads and asserts exact counts. Both tests caught real bugs before they could silently corrupt production data.

---

## Running it

**Requirements:** Java 17+, Maven.

```bash
# Build and run all tests (4 seconds)
mvn test

# Full pipeline demo: 18,000+ events, 4 partitions, 500 devices, windowed aggregation
mvn exec:java "-Dexec.mainClass=com.yourname.dlog.bench.PipelineDemo"

# Benchmark: throughput at 1k / 5k / 10k / 20k events/sec
mvn exec:java "-Dexec.mainClass=com.yourname.dlog.bench.BenchmarkRunner"

# Headline demo: 2,000 ev/s, leader killed at t=5s, zero data loss
mvn exec:java "-Dexec.mainClass=com.yourname.dlog.bench.FailoverUnderLoadDemo"
```

### Manual failover demo (3 terminals)

```bash
# Terminal 1 - leader for partition 0, replicating to follower on 9003
mvn exec:java "-Dexec.mainClass=com.yourname.dlog.broker.BrokerServer" "-Dexec.args=9001 0:L localhost:9003:0"

# Terminal 2 - follower, watches leader and self-promotes if it dies
mvn exec:java "-Dexec.mainClass=com.yourname.dlog.broker.FailoverDemo"

# Terminal 3 - continuous producer; kill Terminal 1 partway through and watch recovery
mvn exec:java "-Dexec.mainClass=com.yourname.dlog.broker.ManualClientTest"
```

---

## Test suite

| Test | What it proves |
|---|---|
| `PartitionLogTest.concurrentAppends_produceNoGapsAndNoDuplicates` | Thread-safe log under 10 concurrent writers — no lost or duplicate offsets |
| `PartitionLogTest.fetchFrom_returnsOnlyRecordsAtOrAfterGivenOffset` | Correct offset-based fetch semantics |
| `StreamAggregatorTest.concurrentUpdates_sameDeviceAndWindow_neverLoseUpdates` | No lost aggregation updates under 8 concurrent threads hitting the same key |
| `StreamAggregatorTest.lateEvents_beyondGracePeriod_areRejectedAndCounted` | Late-event rejection policy works correctly |
| `StreamAggregatorTest.flushCompletedWindows_returnsCorrectAggregatesAndClearsState` | Window flush produces correct count/avg/max and clears state |
| `FailoverIntegrationTest` | End-to-end: 200 writes, leader killed at write 80, zero data loss, zero duplicates, gap-free ordering |

```bash
mvn test
# Tests run: 7, Failures: 0, Errors: 0 — completes in ~8 seconds
```

---

## Tech stack

| | |
|---|---|
| Language | Java 17 |
| Build | Maven |
| Testing | JUnit 5 |
| Serialization | Jackson (JSON wire protocol) |
| Networking | Plain TCP sockets with persistent connections + TCP_NODELAY |
| Concurrency | `ReentrantLock`, `ConcurrentHashMap`, `AtomicLong`, `ExecutorService`, `ScheduledExecutorService`, `CountDownLatch`, `CopyOnWriteArrayList` |

No external messaging or streaming frameworks — the log, replication protocol, failure detection, consumer workers, and aggregation engine are all implemented from scratch.

---

## Project structure
src/main/java/com/yourname/dlog/

├── log/

│   └── PartitionLog.java          # Thread-safe append-only log

├── broker/

│   ├── BrokerServer.java          # TCP broker, leader/follower role

│   ├── BrokerClient.java          # Persistent-connection client

│   ├── Producer.java              # Partitioned producer with failover

│   ├── ClusterMetadata.java       # Partition → leader mapping

│   ├── Partitioner.java           # Key → partition routing

│   ├── HeartbeatMonitor.java      # Peer liveness detection

│   ├── FailoverCoordinator.java   # Promotes follower on leader death

│   ├── Request.java / Response.java

│   └── ManualClientTest.java / FailoverDemo.java

├── consumer/

│   └── ConsumerWorker.java        # Per-partition polling worker

├── aggregator/

│   ├── StreamAggregator.java      # ConcurrentHashMap windowed state

│   ├── WindowAggregate.java       # Immutable accumulator

│   ├── WindowKey.java             # (device, window) key

│   └── WindowFlusher.java         # Scheduled flush + late-event handling

└── bench/

├── EventGenerator.java        # Synthetic IoT sensor stream

├── SensorEvent.java           # Immutable event type

├── EventSerializer.java       # Thread-safe Jackson wrapper

├── LatencyTracker.java        # p50/p95/p99 latency measurement

├── BenchmarkRunner.java       # Rate-limited multi-level benchmark

├── PipelineDemo.java          # Full pipeline end-to-end demo

├── FailoverUnderLoadDemo.java  # Headline failure-survival demo

└── GeneratorPreview.java      # Quick generator sanity check

---

