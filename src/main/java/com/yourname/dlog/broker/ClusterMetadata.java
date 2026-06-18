package com.yourname.dlog.broker;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks which broker (host:port) currently leads each partition.
 * Hardcoded/static for now (Day 3) - this becomes dynamic once we add
 * failover in Day 6, which is exactly why it's a ConcurrentHashMap
 * already: the leader for a partition can change at runtime, read by
 * producer threads and written by failover logic concurrently.
 */
public class ClusterMetadata {

    public static class BrokerAddress {
        public final String host;
        public final int port;
        public BrokerAddress(String host, int port) {
            this.host = host;
            this.port = port;
        }
        @Override public String toString() { return host + ":" + port; }
    }

    private final Map<Integer, BrokerAddress> partitionLeader = new ConcurrentHashMap<>();
    private final int numPartitions;

    public ClusterMetadata(int numPartitions) {
        this.numPartitions = numPartitions;
    }

    public void setLeader(int partition, String host, int port) {
        partitionLeader.put(partition, new BrokerAddress(host, port));
    }

    public BrokerAddress getLeader(int partition) {
        return partitionLeader.get(partition);
    }

    public int numPartitions() {
        return numPartitions;
    }
}