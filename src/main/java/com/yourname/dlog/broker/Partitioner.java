package com.yourname.dlog.broker;

/**
 * Decides which partition a given key belongs to.
 * Using key.hashCode() means the SAME key always maps to the SAME
 * partition, which matters: it's how you get ordering guarantees per key
 * later (e.g. all events for "device-42" always land in the same
 * partition, so they're always read back in the order they were written).
 */
public class Partitioner {

    public static int partitionFor(String key, int numPartitions) {
        int hash = key.hashCode();
        // Math.abs alone isn't enough: Integer.MIN_VALUE has no positive
        // counterpart, so we mask the sign bit off explicitly instead.
        int nonNegativeHash = hash & 0x7fffffff;
        return nonNegativeHash % numPartitions;
    }
}