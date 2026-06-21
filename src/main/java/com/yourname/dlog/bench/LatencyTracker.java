package com.yourname.dlog.bench;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Records per-event end-to-end latency (time from event generation to
 * successful produce acknowledgment) and computes percentiles on demand.
 *
 * Thread-safe: multiple producer threads can call record() concurrently.
 * Uses a synchronized list for simplicity at this scale - in a production
 * benchmark you'd use HdrHistogram for lock-free recording, but the
 * overhead here is negligible compared to the network round-trips we're
 * actually measuring.
 */
public class LatencyTracker {

    private final List<Long> latenciesMs = Collections.synchronizedList(new ArrayList<>());
    private final AtomicLong totalRecorded = new AtomicLong(0);
    private final AtomicLong totalFailed = new AtomicLong(0);

    public void record(long startNanos, boolean success) {
        long latencyMs = (System.nanoTime() - startNanos) / 1_000_000;
        if (success) {
            latenciesMs.add(latencyMs);
            totalRecorded.incrementAndGet();
        } else {
            totalFailed.incrementAndGet();
        }
    }

    public long percentile(double p) {
        List<Long> sorted;
        synchronized (latenciesMs) {
            if (latenciesMs.isEmpty()) return 0;
            sorted = new ArrayList<>(latenciesMs);
        }
        Collections.sort(sorted);
        int index = (int) Math.ceil(p / 100.0 * sorted.size()) - 1;
        return sorted.get(Math.max(0, index));
    }

    public long getTotal() { return totalRecorded.get(); }
    public long getFailed() { return totalFailed.get(); }

    public String summary() {
        return String.format(
                "total=%d failed=%d p50=%dms p95=%dms p99=%dms",
                getTotal(), getFailed(),
                percentile(50), percentile(95), percentile(99));
    }
}