package com.yourname.dlog.log;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A single immutable entry in the log.
 * Immutability matters here: once a record exists, multiple threads
 * (replication senders, consumers, the log itself) may read it concurrently
 * with zero risk of seeing a half-written object.
 *
 * @JsonCreator + @JsonProperty tell Jackson to build instances of this
 * class via this constructor when deserializing JSON back into a Record
 * (e.g. on the client side after a FETCH response comes back over the
 * wire). Without this, Jackson has no way to construct a Record from
 * JSON because the class has no no-arg constructor and no setters -
 * which is exactly what keeps it immutable.
 */
public final class Record implements Serializable {
    private final long offset;
    private final String key;
    private final String value;
    private final long timestamp;

    @JsonCreator
    public Record(
            @JsonProperty("offset") long offset,
            @JsonProperty("key") String key,
            @JsonProperty("value") String value,
            @JsonProperty("timestamp") long timestamp) {
        this.offset = offset;
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
    }

    public long getOffset() { return offset; }
    public String getKey() { return key; }
    public String getValue() { return value; }
    public long getTimestamp() { return timestamp; }

    @Override
    public String toString() {
        return "Record{offset=" + offset + ", key='" + key + "', value='" + value +
                "', timestamp=" + timestamp + "}";
    }
}