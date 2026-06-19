package com.yourname.dlog.aggregator;

import java.util.Objects;

/**
 * Identifies a unique (device, time-window) combination.
 * Used as the key in the aggregation state map.
 *
 * Immutable and correctly implements equals/hashCode - both are
 * required for correctness as a HashMap/ConcurrentHashMap key.
 * Forgetting to override either is a classic Java bug: without them,
 * two WindowKey instances with the same deviceId and windowStart would
 * be treated as different keys (using Object's identity-based equals),
 * causing the aggregation map to accumulate duplicate entries instead
 * of merging updates for the same window.
 */
public final class WindowKey {

    public final String deviceId;
    public final long windowStart; // epoch ms, always a multiple of WINDOW_SIZE_MS

    public static final long WINDOW_SIZE_MS = 10_000; // 10-second tumbling windows

    public WindowKey(String deviceId, long eventTimestamp) {
        this.deviceId = deviceId;
        // Bucket the event's timestamp to its window's start time.
        // e.g. an event at t=15300ms lands in window [10000, 20000),
        // so windowStart = 15300 - (15300 % 10000) = 10000.
        this.windowStart = eventTimestamp - (eventTimestamp % WINDOW_SIZE_MS);
    }

    public long windowEnd() {
        return windowStart + WINDOW_SIZE_MS;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof WindowKey)) return false;
        WindowKey other = (WindowKey) o;
        return windowStart == other.windowStart && Objects.equals(deviceId, other.deviceId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(deviceId, windowStart);
    }

    @Override
    public String toString() {
        return deviceId + "@[" + windowStart + "," + windowEnd() + ")";
    }
}