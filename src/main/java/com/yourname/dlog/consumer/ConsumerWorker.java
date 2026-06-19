package com.yourname.dlog.consumer;

import com.yourname.dlog.bench.EventSerializer;
import com.yourname.dlog.bench.SensorEvent;
import com.yourname.dlog.broker.BrokerClient;
import com.yourname.dlog.broker.Request;
import com.yourname.dlog.broker.Response;

import java.util.List;
import java.util.function.Consumer;

/**
 * A Runnable that continuously polls one partition for new records
 * and hands each deserialized SensorEvent to a caller-supplied callback.
 *
 * Each ConsumerWorker owns EXACTLY ONE partition and is the only thread
 * reading from that partition. This is a deliberate design choice:
 * - It preserves per-partition ordering guarantees (records are always
 *   processed in the order they were written, since only one thread reads
 *   the partition sequentially).
 * - It avoids the complexity of coordinating multiple threads reading the
 *   same partition (you'd need to track which offsets were already processed,
 *   handle gaps if one thread is slow, etc.)
 * The trade-off: parallelism comes from having MANY workers across MANY
 * partitions, not from multiple threads on one partition. This is exactly
 * how Kafka's consumer groups work: one partition = one consumer at a time.
 */
public class ConsumerWorker implements Runnable {

    private final int partition;
    private final BrokerClient brokerClient;
    private final Consumer<SensorEvent> eventHandler;
    private volatile boolean running = true;
    private long nextOffset = 0;

    // Stats for throughput reporting
    private long eventsProcessed = 0;
    private long startTime = 0;

    public ConsumerWorker(int partition, String brokerHost, int brokerPort,
                          Consumer<SensorEvent> eventHandler) {
        this.partition = partition;
        this.brokerClient = new BrokerClient(brokerHost, brokerPort);
        this.eventHandler = eventHandler;
    }

    @Override
    public void run() {
        startTime = System.currentTimeMillis();
        System.out.println("ConsumerWorker started for partition " + partition);

        while (running) {
            try {
                Response response = brokerClient.send(Request.fetch(partition, nextOffset));

                if (!response.success) {
                    System.err.println("Fetch error on partition " + partition + ": " + response.error);
                    Thread.sleep(200);
                    continue;
                }

                List<com.yourname.dlog.log.Record> records = response.records;
                if (records == null || records.isEmpty()) {
                    // No new records yet - brief pause before polling again.
                    // This is a simple polling model; a production system would
                    // use long-polling or server-push to avoid this busy-wait.
                    Thread.sleep(50);
                    continue;
                }

                for (com.yourname.dlog.log.Record record : records) {
                    SensorEvent event = EventSerializer.deserialize(record.getValue());
                    eventHandler.accept(event);
                    eventsProcessed++;
                    nextOffset = record.getOffset() + 1;
                }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                if (running) {
                    System.err.println("ConsumerWorker partition " + partition + " error: " + e.getMessage());
                    try { Thread.sleep(200); } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }
        System.out.println("ConsumerWorker partition " + partition + " stopped. Processed " + eventsProcessed + " events.");
    }

    public void stop() { running = false; }
    public long getEventsProcessed() { return eventsProcessed; }
    public long getStartTime() { return startTime; }
    public int getPartition() { return partition; }
}