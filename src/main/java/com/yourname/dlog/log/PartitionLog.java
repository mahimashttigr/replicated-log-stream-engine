package com.yourname.dlog.log;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/**
 * An append-only, ordered log for one partition.
 *
 * Concurrency design (v1 - simple lock):
 *   - A single ReentrantLock guards both offset assignment and the append
 *     to the backing list. This guarantees: (a) offsets are assigned in
 *     strictly increasing order with no gaps or duplicates, and
 *     (b) the backing list never sees a torn/partial write.
 *   - Reads (fetchFrom) do NOT take the lock. ArrayList reads by index are
 *     safe to do concurrently with writes IF the list never resizes
 *     incorrectly under concurrent access - which is why writes are
 *     serialized through the lock. This is a deliberate tradeoff:
 *     readers don't block each other or get blocked by writers for long,
 *     but you should be ready to explain why this is safe (the lock fully
 *     orders all mutations; reads only ever see a fully-appended list).
 *
 * We will compare this against an AtomicLong + lock-free structure later
 * and measure the throughput difference - keep this version as a baseline.
 */
public class PartitionLog {

    private final List<Record> records = new ArrayList<>();
    private final ReentrantLock lock = new ReentrantLock();
    private long nextOffset = 0;
    private final Map<String, Long> lastSequenceByProducer = new java.util.HashMap<>();
    private final Map<String, Long> lastOffsetByProducer = new java.util.HashMap<>();
    /**
     * Appends a record, assigning it the next offset.
     * Thread-safe: safe to call from many threads concurrently.
     *
     * @return the offset assigned to this record
     */
    public long append(String key, String value, long timestamp) {
        lock.lock();
        try {
            long offset = nextOffset;
            Record record = new Record(offset, key, value, timestamp);
            records.add(record);
            nextOffset++;
            return offset;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns all records with offset >= fromOffset, in order.
     * Safe to call concurrently with append() and with other fetchFrom() calls.
     */
    public List<Record> fetchFrom(long fromOffset) {
        lock.lock();
        try {
            if (fromOffset >= records.size()) {
                return Collections.emptyList();
            }
            int start = (int) Math.max(0, fromOffset);
            return new ArrayList<>(records.subList(start, records.size()));
        } finally {
            lock.unlock();
        }
    }

    public long size() {
        lock.lock();
        try {
            return records.size();
        } finally {
            lock.unlock();
        }
    }

    public long nextOffset() {
        lock.lock();
        try {
            return nextOffset;
        } finally {
            lock.unlock();
        }
    }
    /**
     * Appends a record at a SPECIFIC, caller-provided offset rather than
     * assigning the next one automatically. Used by followers replicating
     * from a leader: the follower must store records at the exact same
     * offsets the leader assigned, or the two copies of the log would
     * diverge and stop matching record-for-record.
     *
     * Thread-safe for the same reasons as append(): the whole
     * check-and-append sequence happens while holding the lock.
     */
public void appendAt(long offset, String key, String value, long timestamp, String producerId, long sequenceNumber) {
        lock.lock();
        try {
            if (offset != nextOffset) {
                throw new IllegalStateException(
                    "Out-of-order replication: expected offset " + nextOffset +
                    " but got " + offset + "."
                );
            }
            records.add(new Record(offset, key, value, timestamp));
            nextOffset++;
            lastSequenceByProducer.put(producerId, sequenceNumber);
            lastOffsetByProducer.put(producerId, offset);
        } finally {
            lock.unlock();
        }
    }
     // producerId -> highest sequence number successfully applied for this partition

    /**
     * Returns true if this (producerId, sequenceNumber) has already been
     * applied to this log - i.e. this is a duplicate retry, safe to ignore.
     * Must be called while already holding the log's lock by the caller,
     * since it reads/writes lastSequenceByProducer alongside the actual
     * append - see appendIdempotent below.
     */
    private boolean isDuplicate(String producerId, long sequenceNumber) {
        Long last = lastSequenceByProducer.get(producerId);
        return last != null && sequenceNumber <= last;
    }
    public long appendIdempotent(String key, String value, long timestamp, String producerId, long sequenceNumber) {
        lock.lock();
        try {
            if (isDuplicate(producerId, sequenceNumber)) {
                return lastOffsetByProducer.getOrDefault(producerId, -1L);
            }

            long offset = nextOffset;
            records.add(new Record(offset, key, value, timestamp));
            nextOffset++;
            lastSequenceByProducer.put(producerId, sequenceNumber);
            lastOffsetByProducer.put(producerId, offset);
            return offset;
        } finally {
            lock.unlock();
        }
    }
}