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
import com.yourname.dlog.broker.Response;
import com.yourname.dlog.consumer.ConsumerWorker;

public class PipelineDemo {

    static final int NUM_PARTITIONS = 4;
    static final int BROKER_BASE_PORT = 18000;
    static final int NUM_DEVICES = 500;
    static final double LATE_EVENT_RATE = 0.03;

    public static void main(String[] args) throws Exception {

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
        Thread.sleep(500);

        // --- Cluster metadata ---
        ClusterMetadata metadata = new ClusterMetadata(NUM_PARTITIONS);
        for (int p = 0; p < NUM_PARTITIONS; p++) {
            metadata.setLeader(p, "localhost", BROKER_BASE_PORT + p);
        }

        // --- Shared aggregator + flusher ---
        // 5 second grace period: late events up to 5s behind are still accepted.
        StreamAggregator aggregator = new StreamAggregator(5_000);

        WindowFlusher flusher = new WindowFlusher(
                aggregator,
                window -> System.out.printf(
                        "[WINDOW] %-20s | count=%4d | avg=%5.1f°C | max=%5.1f°C%n",
                        window.key.toString(),
                        window.aggregate.getCount(),
                        window.aggregate.getAverage(),
                        window.aggregate.getMax()),
                2_000 // check every 2 seconds
        );
        flusher.start();

        // --- Consumer workers, one per partition, all feeding same aggregator ---
        AtomicLong totalConsumed = new AtomicLong(0);
        ExecutorService consumerPool = Executors.newFixedThreadPool(NUM_PARTITIONS);
        List<ConsumerWorker> workers = new ArrayList<>();

        for (int p = 0; p < NUM_PARTITIONS; p++) {
            ConsumerWorker worker = new ConsumerWorker(
                    p,
                    "localhost",
                    BROKER_BASE_PORT + p,
                    event -> {
                        aggregator.update(event);
                        totalConsumed.incrementAndGet();
                    }
            );
            workers.add(worker);
            consumerPool.submit(worker);
        }

        // --- Produce for 25 seconds (covers ~2 full 10s windows + partial 3rd) ---
        Producer producer = new Producer(metadata);
        EventGenerator generator = new EventGenerator(NUM_DEVICES, LATE_EVENT_RATE);

        System.out.println("Producing events for 25 seconds...");
        long produceStart = System.currentTimeMillis();
        long totalProduced = 0;

        while (System.currentTimeMillis() - produceStart < 25_000) {
            SensorEvent event = generator.generateOne();
            Response r = producer.send(event.getDeviceId(), EventSerializer.serialize(event));
            if (r.success) totalProduced++;
        }

        System.out.println("\nProduction complete: " + totalProduced + " events produced.");
        System.out.println("Waiting for consumers and final window flush...");
        Thread.sleep(5_000); // let consumers catch up and final windows close

        // --- Shutdown ---
        flusher.stop(); // triggers one final flush before stopping
        workers.forEach(ConsumerWorker::stop);
        consumerPool.shutdown();
        consumerPool.awaitTermination(5, TimeUnit.SECONDS);
        brokers.forEach(b -> { try { b.stop(); } catch (Exception ignored) {} });

        // --- Final summary ---
        long consumed = totalConsumed.get();
        double elapsedSec = (System.currentTimeMillis() - produceStart) / 1000.0;
        System.out.println("\n=== Final Pipeline Summary ===");
        System.out.println("Total produced       : " + totalProduced);
        System.out.println("Total consumed       : " + consumed);
        System.out.println("Lost events          : " + (totalProduced - consumed));
        System.out.println("Late events rejected : " + aggregator.getTotalLateEvents());
        System.out.println("Windows flushed      : " + flusher.getTotalWindowsFlushed());
        System.out.printf( "Throughput           : %.0f events/sec%n", totalProduced / elapsedSec);
    }
}