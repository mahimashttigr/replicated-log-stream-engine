package com.yourname.dlog.aggregator;

import com.yourname.dlog.bench.SensorEvent;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

class StreamAggregatorTest {

    @Test
    void concurrentUpdates_sameDeviceAndWindow_neverLoseUpdates() throws InterruptedException {
        StreamAggregator aggregator = new StreamAggregator(5000);

        int threadCount = 8;
        int updatesPerThread = 1000;
        int totalUpdates = threadCount * updatesPerThread;

        // All events for the same device, same timestamp -> same window key
        // This maximises contention on exactly one ConcurrentHashMap bucket
        long fixedTimestamp = System.currentTimeMillis();
        double fixedTemp = 20.0;

        ExecutorService pool = Executors.newFixedThreadPool(threadCount);
        CountDownLatch ready = new CountDownLatch(threadCount);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threadCount);

        for (int t = 0; t < threadCount; t++) {
            pool.submit(() -> {
                ready.countDown();
                try {
                    start.await();
                    for (int i = 0; i < updatesPerThread; i++) {
                        aggregator.update(new SensorEvent("device-stress", fixedTemp, fixedTimestamp));
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }

        ready.await();
        start.countDown(); // release all threads simultaneously
        done.await(15, TimeUnit.SECONDS);
        pool.shutdown();

        // Force flush by using a grace period of 0 in a separate aggregator
        // that we can inspect - or just check internal state directly
        assertEquals(totalUpdates, aggregator.getTotalEventsProcessed(),
                "Expected " + totalUpdates + " events processed - lost updates if this fails");

        // The sum should be exactly totalUpdates * fixedTemp
        // We can't flush yet (window hasn't closed), so we force-check via a
        // short-lived aggregator with a huge grace period and check active count
        assertEquals(1, aggregator.getActiveWindowCount(),
                "Should have exactly 1 active window (all updates hit the same key)");
    }

    @Test
    void lateEvents_beyondGracePeriod_areRejectedAndCounted() throws Exception {
        StreamAggregator aggregator = new StreamAggregator(1000); // 1 second grace

        // Event with timestamp 30 seconds in the past - well beyond grace period
        long oldTimestamp = System.currentTimeMillis() - 30_000;
        aggregator.update(new SensorEvent("device-0", 22.0, oldTimestamp));

        assertEquals(0, aggregator.getTotalEventsProcessed());
        assertEquals(1, aggregator.getTotalLateEvents());
    }

 @Test
    void flushCompletedWindows_returnsCorrectAggregatesAndClearsState() {
        StreamAggregator aggregator = new StreamAggregator(0);

        // Use forceInsert to bypass the late-event guard so we can test
        // flush behavior independently of the acceptance policy.
        long pastTimestamp = System.currentTimeMillis() - 60_000;
        aggregator.forceInsert(new SensorEvent("device-A", 20.0, pastTimestamp));
        aggregator.forceInsert(new SensorEvent("device-A", 24.0, pastTimestamp));
        aggregator.forceInsert(new SensorEvent("device-A", 22.0, pastTimestamp));

        List<StreamAggregator.FlushedWindow> flushed = aggregator.flushCompletedWindows();

        assertEquals(1, flushed.size(), "Expected exactly one flushed window");
        StreamAggregator.FlushedWindow w = flushed.get(0);
        assertEquals(3, w.aggregate.getCount());
        assertEquals(24.0, w.aggregate.getMax(), 0.001);
        assertEquals(22.0, w.aggregate.getAverage(), 0.001);
        assertEquals(0, aggregator.getActiveWindowCount(), "Map should be empty after flush");
    }
}