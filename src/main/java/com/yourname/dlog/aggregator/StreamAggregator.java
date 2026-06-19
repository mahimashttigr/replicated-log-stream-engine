package com.yourname.dlog.aggregator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.yourname.dlog.bench.SensorEvent;

/**
 * Maintains live windowed aggregation state for all active (device, window)
 * combinations, and flushes completed windows on request.
 *
 * CONCURRENCY DESIGN:
 *
 * 1. activeWindows is a ConcurrentHashMap. Multiple ConsumerWorker threads
 *    call update() concurrently. The critical operation is:
 *    "if this (device, window) key exists, combine the new value into
 *    the existing aggregate; if not, create a new one."
 *    This is a read-modify-write that MUST be atomic. merge() performs
 *    it atomically per bucket - two threads updating the SAME key cannot
 *    interleave in a way that loses either update.
 *
 * 2. WindowAggregate is IMMUTABLE. combine() returns a NEW instance
 *    rather than mutating the existing one. This is essential: merge()
 *    is atomic at the map level (which object gets stored) but NOT at
 *    the object level (mutations inside the remapping function are not
 *    protected). A mutable combine() would cause lost updates under
 *    high concurrency even with ConcurrentHashMap - exactly the bug
 *    our stress test caught (8000 expected, 7979 actual) before the fix.
 *
 * 3. totalEventsProcessed and totalLateEvents are AtomicLong because
 *    they are incremented by multiple threads concurrently. A plain long
 *    with ++ is a non-atomic read-modify-write that loses increments
 *    under contention - again caught by the stress test before the fix.
 */
public class StreamAggregator {

    private final ConcurrentHashMap<WindowKey, WindowAggregate> activeWindows
            = new ConcurrentHashMap<>();

    private final AtomicLong totalEventsProcessed = new AtomicLong(0);
    private final AtomicLong totalLateEvents = new AtomicLong(0);
    private final long gracePeriodMs;

    public StreamAggregator(long gracePeriodMs) {
        this.gracePeriodMs = gracePeriodMs;
    }

    /**
     * Updates aggregation state for the event's (device, window) pair.
     * Safe to call from multiple threads concurrently.
     */
    public void update(SensorEvent event) {
        WindowKey key = new WindowKey(event.getDeviceId(), event.getTimestamp());
        long now = System.currentTimeMillis();

        // Reject events whose window has already closed past the grace period.
        // Policy decision: drop-and-count rather than re-open closed windows,
        // trading precision on very late events for simplicity of not having
        // to re-emit already-flushed results.
        if (key.windowEnd() + gracePeriodMs < now) {
            totalLateEvents.incrementAndGet();
            return;
        }

        activeWindows.merge(
                key,
                new WindowAggregate(event.getTemperature()),
                WindowAggregate::combine
        );

        totalEventsProcessed.incrementAndGet();
    }

    /**
     * Test-only: insert an event bypassing the late-event guard.
     * Package-private so only tests in the same package can call it.
     * Allows testing flush behavior independently of acceptance policy.
     */
    void forceInsert(SensorEvent event) {
        WindowKey key = new WindowKey(event.getDeviceId(), event.getTimestamp());
        activeWindows.merge(
                key,
                new WindowAggregate(event.getTemperature()),
                WindowAggregate::combine
        );
        totalEventsProcessed.incrementAndGet();
    }

    /**
     * Flushes all windows whose end time + grace period has passed.
     * Returns the flushed results and removes them from active state.
     * Call this periodically from a scheduler (Day 11).
     */
    public List<FlushedWindow> flushCompletedWindows() {
        long now = System.currentTimeMillis();
        List<FlushedWindow> flushed = new ArrayList<>();

        for (Map.Entry<WindowKey, WindowAggregate> entry : activeWindows.entrySet()) {
            WindowKey key = entry.getKey();
            if (key.windowEnd() + gracePeriodMs <= now) {
                WindowAggregate agg = activeWindows.remove(key);
                if (agg != null) {
                    flushed.add(new FlushedWindow(key, agg));
                }
            }
        }

        return flushed;
    }

    public long getTotalEventsProcessed() { return totalEventsProcessed.get(); }
    public long getTotalLateEvents() { return totalLateEvents.get(); }
    public int getActiveWindowCount() { return activeWindows.size(); }

    /** Immutable result of a flushed window - safe to pass around freely. */
    public static class FlushedWindow {
        public final WindowKey key;
        public final WindowAggregate aggregate;

        public FlushedWindow(WindowKey key, WindowAggregate aggregate) {
            this.key = key;
            this.aggregate = aggregate;
        }

        @Override
        public String toString() {
            return "Window[" + key + "] -> " + aggregate;
        }
    }
}