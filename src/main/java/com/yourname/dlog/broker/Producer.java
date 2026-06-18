package com.yourname.dlog.broker;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Producer {

    private final ClusterMetadata metadata;
    private final Map<String, BrokerClient> clientCache = new HashMap<>();
    private final Map<Integer, ClusterMetadata.BrokerAddress> failoverOverrides = new HashMap<>();
    private final String producerId = java.util.UUID.randomUUID().toString();
    private long sequenceCounter = 0;

    public Producer(ClusterMetadata metadata) {
        this.metadata = metadata;
    }

    public void setFailoverTarget(int partition, String host, int port) {
        failoverOverrides.put(partition, new ClusterMetadata.BrokerAddress(host, port));
    }

    private ClusterMetadata.BrokerAddress currentLeaderFor(int partition) {
        return metadata.getLeader(partition);
    }

    public Response send(String key, String value) throws IOException {
        int partition = Partitioner.partitionFor(key, metadata.numPartitions());
        long seq = sequenceCounter++;
        return sendWithRetry(partition, key, value, seq, 5);
    }

    private Response sendWithRetry(int partition, String key, String value, long seq, int attemptsLeft) throws IOException {
        ClusterMetadata.BrokerAddress leader = currentLeaderFor(partition);
        if (leader == null) {
            return Response.error("No known leader for partition " + partition);
        }

        BrokerClient client = clientCache.computeIfAbsent(
                leader.toString(), addr -> new BrokerClient(leader.host, leader.port));

        Response response;
        try {
            response = client.send(Request.append(partition, key, value, producerId, seq));
        } catch (IOException e) {
            response = Response.error("NOT_LEADER: connection failed - " + e.getMessage());
        }

        boolean isNotLeaderError = !response.success && response.error != null
                && response.error.startsWith("NOT_LEADER");

        if (isNotLeaderError) {
            if (attemptsLeft <= 1) {
                return response;
            }

            ClusterMetadata.BrokerAddress backup = failoverOverrides.get(partition);
            if (backup != null && !backup.toString().equals(leader.toString())) {
                System.out.println(">>> Producer detected dead leader for partition " + partition +
                        ", failing over to " + backup);
                metadata.setLeader(partition, backup.host, backup.port);
            } else {
                try {
                    Thread.sleep(300);
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
            }

            return sendWithRetry(partition, key, value, seq, attemptsLeft - 1);
        }

        return response;
    }

    public Response fetch(int partition, long fromOffset) throws IOException {
        ClusterMetadata.BrokerAddress leader = currentLeaderFor(partition);
        if (leader == null) return Response.error("No known leader for partition " + partition);
        BrokerClient client = clientCache.computeIfAbsent(
                leader.toString(), addr -> new BrokerClient(leader.host, leader.port));
        return client.send(Request.fetch(partition, fromOffset));
    }
}