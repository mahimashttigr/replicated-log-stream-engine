package com.yourname.dlog.bench;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Thin wrapper around Jackson for SensorEvent serialization.
 * One static ObjectMapper is safe to share across threads - Jackson's
 * ObjectMapper is thread-safe for read operations (serialization and
 * deserialization) once configured, so a single shared instance is
 * both correct and more efficient than creating a new one per call.
 */
public class EventSerializer {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static String serialize(SensorEvent event) {
        try {
            return MAPPER.writeValueAsString(event);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize SensorEvent", e);
        }
    }

    public static SensorEvent deserialize(String json) {
        try {
            return MAPPER.readValue(json, SensorEvent.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize SensorEvent: " + json, e);
        }
    }
}