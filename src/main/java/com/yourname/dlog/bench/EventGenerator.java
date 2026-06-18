package com.yourname.dlog.bench;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Generates synthetic IoT sensor events at a configurable rate, with
 * realistic key cardinality and a small percentage of deliberately
 * out-of-order ("late") timestamps - because real streams are never
 * perfectly ordered, and the aggregation layer needs to handle that
 * honestly rather than being tested against an artificially easy input.
 */
public class EventGenerator {

    private final int numDevices;
    private final double lateEventRate; // e.g. 0.03 = 3% of events arrive "late"
    private final double[] baselineTemps; // each device has its own realistic baseline

    public EventGenerator(int numDevices, double lateEventRate) {
        this.numDevices = numDevices;
        this.lateEventRate = lateEventRate;
        this.baselineTemps = new double[numDevices];
        Random seedRandom = new Random(42); // fixed seed for reproducible baselines across runs
        for (int i = 0; i < numDevices; i++) {
            baselineTemps[i] = 15 + seedRandom.nextDouble() * 15; // baseline between 15-30 C
        }
    }

    /**
     * Generates one event "as if produced right now" (timestamp = now),
     * except for the lateEventRate fraction of calls, which backdate the
     * timestamp to simulate a reading that arrives out of order.
     *
     * Safe to call from multiple threads concurrently: ThreadLocalRandom
     * is specifically designed for this - unlike java.util.Random, it
     * avoids contention between threads because each thread gets its own
     * generator instance under the hood, so there's no shared mutable
     * state here needing a lock.
     */
    public SensorEvent generateOne() {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        int deviceIndex = rnd.nextInt(numDevices);
        String deviceId = "device-" + deviceIndex;

        double noise = (rnd.nextDouble() - 0.5) * 2.0; // +/- 1.0 degree of noise
        double temperature = Math.round((baselineTemps[deviceIndex] + noise) * 10) / 10.0;

        long now = System.currentTimeMillis();
        long timestamp = now;

        if (rnd.nextDouble() < lateEventRate) {
            // Simulate a late-arriving event: its "true" timestamp is
            // somewhere in the past (1-8 seconds ago), even though we're
            // generating/sending it right now.
            long lateBySeconds = 1 + rnd.nextInt(8);
            timestamp = now - (lateBySeconds * 1000);
        }

        return new SensorEvent(deviceId, temperature, timestamp);
    }
}