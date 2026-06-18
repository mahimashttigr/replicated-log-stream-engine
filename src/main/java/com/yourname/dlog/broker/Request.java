package com.yourname.dlog.broker;

public class Request {
    public String type;
    public int partition;
    public String key;
    public String value;
    public long fromOffset;
    public long replicaOffset;
    public long timestamp;
    public String producerId;
    public long sequenceNumber;

    public Request() {}

    public static Request append(int partition, String key, String value, String producerId, long sequenceNumber) {
        Request r = new Request();
        r.type = "APPEND";
        r.partition = partition;
        r.key = key;
        r.value = value;
        r.producerId = producerId;
        r.sequenceNumber = sequenceNumber;
        return r;
    }

    public static Request fetch(int partition, long fromOffset) {
        Request r = new Request();
        r.type = "FETCH";
        r.partition = partition;
        r.fromOffset = fromOffset;
        return r;
    }

    public static Request replicate(int partition, long offset, String key, String value, long timestamp,
                                       String producerId, long sequenceNumber) {
        Request r = new Request();
        r.type = "REPLICATE";
        r.partition = partition;
        r.replicaOffset = offset;
        r.key = key;
        r.value = value;
        r.timestamp = timestamp;
        r.producerId = producerId;
        r.sequenceNumber = sequenceNumber;
        return r;
    }

    public static Request heartbeat() {
        Request r = new Request();
        r.type = "HEARTBEAT";
        return r;
    }
}