package com.yourname.dlog.bench;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.yourname.dlog.aggregator.StreamAggregator;
import com.yourname.dlog.aggregator.WindowFlusher;
import com.yourname.dlog.broker.BrokerServer;
import com.yourname.dlog.broker.ClusterMetadata;
import com.yourname.dlog.broker.Producer;
import com.yourname.dlog.consumer.ConsumerWorker;

/**
 * Benchmark harness: runs the full pipeline at a target events/sec rate
 * for a fixed duration and reports throughput + latency percentiles.
 *
 * Rate limiting uses a token-bucket approach: we compute how many nanoseconds
 * should elapse between events at the target rate, and sleep to compensate
 * if we're ahead of schedule. This is more accurate than sleeping a fixed
 * interval because it accounts for the time each send() call actually takes.
 */
public class BenchmarkRunner {

    static final int NUM_PARTITIONS = 4;
    static final int BROKER_BASE_PORT = 17000; // different port range to avoid conflicts
    static final int NUM_DEVICES = 500;
    static final int DURATION_SECONDS = 15;

    public static void main(String[] args) throws Exception {
        int[] targetRates = {1_000, 5_000, 10_000, 20_000};

        System.out.println("=== Benchmark Harness ===");
        System.out.println("Duration per run: " + DURATION_SECONDS + "s");
        System.out.println("Partitions: " + NUM_PARTITIONS);
        System.out.println("Devices: " + NUM_DEVICES);
        System.out.println();
        System.out.printf("%-15s %-15s %-10s %-10s %-10s %-10s%n",
                "Target", "Actual", "p50", "p95", "p99", "Lost");
        System.out.println("-".repeat(70));

        for (int targetRate : targetRates) {
            BenchmarkResult result = runBenchmark(targetRate);
            System.out.printf("%-15s %-15s %-10s %-10s %-10s %-10s%n",
                    targetRate + " ev/s",
                    result.actualRate + " ev/s",
                    result.p50ms + "ms",
                    result.p95ms + "ms",
                    result.p99ms + "ms",
                    result.lost);
            Thread.sleep(2000); // brief pause between runs for GC and port cleanup
        }
    }

    static BenchmarkResult runBenchmark(int targetEventsPerSec) throws Exception {
        // --- Start brokers ---
        List<BrokerServer> brokers = new ArrayList<>();
        for (int p = 0; p < NUM_PARTITIONS; p++) {
            BrokerServer broker = new BrokerServer(
                    BROKER_BASE_PORT + p, new int[]{p}, new boolean[]{true});
            brokers.add(broker);
            Thread t = new Thread(() -> {
                try { broker.start(); } catch (Exception ignored) {}
            });
            t.setDaemon(true);
            t.start();
        }
        Thread.sleep(300);

        // --- Cluster metadata ---
        ClusterMetadata metadata = new ClusterMetadata(NUM_PARTITIONS);
        for (int p = 0; p < NUM_PARTITIONS; p++) {
            metadata.setLeader(p, "localhost", BROKER_BASE_PORT + p);
        }

        // --- Aggregator + flusher (silent - no console output during benchmark) ---
        StreamAggregator aggregator = new StreamAggregator(5_000);
        WindowFlusher flusher = new WindowFlusher(aggregator, w -> {}, 2_000);
        flusher.start();

        // --- Consumer workers ---
        AtomicLong totalConsumed = new AtomicLong(0);
        ExecutorService consumerPool = Executors.newFixedThreadPool(NUM_PARTITIONS);
        List<ConsumerWorker> workers = new ArrayList<>();
        for (int p = 0; p < NUM_PARTITIONS; p++) {
            ConsumerWorker worker = new ConsumerWorker(
                    p, "localhost", BROKER_BASE_PORT + p,
                    event -> {
                        aggregator.update(event);
                        totalConsumed.incrementAndGet();
                    });
            workers.add(worker);
            consumerPool.submit(worker);
        }

        // --- Rate-limited producer ---
        Producer producer = new Producer(metadata);
        EventGenerator generator = new EventGenerator(NUM_DEVICES, 0.03);
        LatencyTracker tracker = new LatencyTracker();

        long intervalNanos = 1_000_000_000L / targetEventsPerSec;
        long benchStart = System.nanoTime();
        long benchEndNanos = benchStart + (long) DURATION_SECONDS * 1_000_000_000L;
        long nextSendNanos = benchStart;
        long totalProduced = 0;

        while (System.nanoTime() < benchEndNanos) {
            long now = System.nanoTime();
            if (now < nextSendNanos) {
                // Sleep only if we're more than 100 microseconds ahead -
                // avoid spinning the CPU for tiny gaps but also avoid
                // oversleeping via Thread.sleep's coarse granularity.
                long sleepNanos = nextSendNanos - now;
                if (sleepNanos > 100_000) {
                    Thread.sleep(sleepNanos / 1_000_000, (int)(sleepNanos % 1_000_000));
                }
                continue;
            }

            SensorEvent event = generator.generateOne();
            long sendStart = System.nanoTime();
            try {
                com.yourname.dlog.broker.Response r = producer.send(
                        event.getDeviceId(), EventSerializer.serialize(event));
                tracker.record(sendStart, r.success);
                if (r.success) totalProduced++;
            } catch (Exception e) {
                tracker.record(sendStart, false);
            }

            nextSendNanos += intervalNanos;
        }

        // Wait for consumers to catch up
        Thread.sleep(2000);

        // --- Collect results ---
        long actualRate = totalProduced / DURATION_SECONDS;
        long lost = totalProduced - totalConsumed.get();

        // --- Shutdown ---
        flusher.stop();
        workers.forEach(ConsumerWorker::stop);
        consumerPool.shutdown();
        consumerPool.awaitTermination(3, TimeUnit.SECONDS);
        brokers.forEach(b -> { try { b.stop(); } catch (Exception ignored) {} });
        Thread.sleep(500); // let ports release

        return new BenchmarkResult(
                actualRate,
                tracker.percentile(50),
                tracker.percentile(95),
                tracker.percentile(99),
                Math.max(0, lost)
        );
    }

    static class BenchmarkResult {
        final long actualRate, p50ms, p95ms, p99ms, lost;
        BenchmarkResult(long actualRate, long p50ms, long p95ms, long p99ms, long lost) {
            this.actualRate = actualRate;
            this.p50ms = p50ms;
            this.p95ms = p95ms;
            this.p99ms = p99ms;
            this.lost = lost;
        }
    }
}