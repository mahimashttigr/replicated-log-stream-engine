package com.yourname.dlog.aggregator;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Periodically calls flushCompletedWindows() on a StreamAggregator and
 * passes each flushed result to a caller-supplied output handler.
 *
 * Runs on a single background thread via ScheduledExecutorService.
 * Same reasoning as HeartbeatMonitor (Day 5): ScheduledExecutorService
 * is preferred over a hand-rolled while(true)+sleep() loop because:
 * - It handles scheduling drift (each tick is scheduled from the
 *   previous tick's START time, not end time, so slow flushes don't
 *   accumulate delay).
 * - If the flush throws an unexpected exception, the executor logs it
 *   and schedules the next tick anyway; a hand-rolled loop would die
 *   silently and stop flushing forever.
 * - Clean shutdown via stop() with no leaked threads.
 */
public class WindowFlusher {

    private final StreamAggregator aggregator;
    private final Consumer<StreamAggregator.FlushedWindow> outputHandler;
    private final long intervalMs;
    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "window-flusher");
                t.setDaemon(true); // don't prevent JVM shutdown
                return t;
            });

    private long totalWindowsFlushed = 0;

    public WindowFlusher(StreamAggregator aggregator,
                         Consumer<StreamAggregator.FlushedWindow> outputHandler,
                         long intervalMs) {
        this.aggregator = aggregator;
        this.outputHandler = outputHandler;
        this.intervalMs = intervalMs;
    }

    public void start() {
        scheduler.scheduleAtFixedRate(this::flush, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
        System.out.println("WindowFlusher started, checking every " + intervalMs + "ms");
    }

    private void flush() {
        List<StreamAggregator.FlushedWindow> flushed = aggregator.flushCompletedWindows();
        for (StreamAggregator.FlushedWindow window : flushed) {
            outputHandler.accept(window);
            totalWindowsFlushed++;
        }
        if (!flushed.isEmpty()) {
            System.out.println("[flusher] Emitted " + flushed.size() +
                    " windows. Total flushed so far: " + totalWindowsFlushed);
        }
    }

    public void stop() {
        // Do one final flush before shutting down to catch any windows
        // that completed between the last scheduled tick and now.
        flush();
        scheduler.shutdown();
        try {
            scheduler.awaitTermination(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public long getTotalWindowsFlushed() { return totalWindowsFlushed; }
}