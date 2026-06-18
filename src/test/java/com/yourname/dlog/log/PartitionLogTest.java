package com.yourname.dlog.log;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class PartitionLogTest {

    @Test
    void concurrentAppends_produceNoGapsAndNoDuplicates() throws InterruptedException {
        PartitionLog log = new PartitionLog();

        int threadCount = 10;
        int appendsPerThread = 1000;
        int totalExpected = threadCount * appendsPerThread;

        ExecutorService pool = Executors.newFixedThreadPool(threadCount);
        CountDownLatch readyLatch = new CountDownLatch(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);

        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            pool.submit(() -> {
                readyLatch.countDown();
                try {
                    startLatch.await(); // all threads start appending at roughly the same instant
                    for (int i = 0; i < appendsPerThread; i++) {
                        log.append("thread-" + threadId, "value-" + i, System.currentTimeMillis());
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        readyLatch.await();      // wait until all threads are spun up
        startLatch.countDown();  // release them all at once - maximizes contention
        doneLatch.await(10, TimeUnit.SECONDS);
        pool.shutdown();

        List<Record> all = log.fetchFrom(0);

        assertEquals(totalExpected, all.size(),
                "Expected exactly " + totalExpected + " records, got " + all.size());

        // No duplicate offsets, no gaps: offsets must be exactly 0..totalExpected-1
        Set<Long> offsets = all.stream().map(Record::getOffset).collect(Collectors.toSet());
        assertEquals(totalExpected, offsets.size(), "Found duplicate offsets");

        for (long expectedOffset = 0; expectedOffset < totalExpected; expectedOffset++) {
            assertTrue(offsets.contains(expectedOffset), "Missing offset " + expectedOffset);
        }

        // Offsets in the returned list must be strictly increasing in order
        for (int i = 1; i < all.size(); i++) {
            assertTrue(all.get(i).getOffset() > all.get(i - 1).getOffset(),
                    "Offsets out of order at index " + i);
        }
    }

    @Test
    void fetchFrom_returnsOnlyRecordsAtOrAfterGivenOffset() {
        PartitionLog log = new PartitionLog();
        for (int i = 0; i < 10; i++) {
            log.append("k" + i, "v" + i, System.currentTimeMillis());
        }

        List<Record> fromFive = log.fetchFrom(5);
        assertEquals(5, fromFive.size());
        assertEquals(5, fromFive.get(0).getOffset());
    }

    @Test
    void fetchFrom_beyondEnd_returnsEmptyList() {
        PartitionLog log = new PartitionLog();
        log.append("k", "v", System.currentTimeMillis());
        assertTrue(log.fetchFrom(100).isEmpty());
    }
}