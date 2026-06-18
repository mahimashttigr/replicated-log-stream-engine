package com.yourname.dlog.bench;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A single IoT sensor reading. Immutable, same reasoning as Record:
 * safe to hand to multiple threads (generator thread produces it,
 * consumer worker threads read it) with zero risk of partial-write
 * visibility bugs.
 */
public final class SensorEvent {
    private final String deviceId;
    private final double temperature;
    private final long timestamp; // event time - when the reading was actually taken

    @JsonCreator
    public SensorEvent(
            @JsonProperty("deviceId") String deviceId,
            @JsonProperty("temperature") double temperature,
            @JsonProperty("timestamp") long timestamp) {
        this.deviceId = deviceId;
        this.temperature = temperature;
        this.timestamp = timestamp;
    }

    public String getDeviceId() { return deviceId; }
    public double getTemperature() { return temperature; }
    public long getTimestamp() { return timestamp; }

    @Override
    public String toString() {
        return "SensorEvent{device='" + deviceId + "', temp=" + temperature + ", ts=" + timestamp + "}";
    }
}