package com.yourname.dlog.broker;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.yourname.dlog.log.PartitionLog;

public class BrokerServer {

    private final int port;
    private final Map<Integer, PartitionLog> partitionLogs = new ConcurrentHashMap<>();
    // For each partition this broker hosts, is it currently the LEADER?
    // Starts as the role declared at startup; can flip true if promoted.
    private final Map<Integer, Boolean> isLeaderFor = new ConcurrentHashMap<>();
    // For partitions this broker LEADS: where is the follower to replicate to?
    private final Map<Integer, ClusterMetadata.BrokerAddress> followerFor = new ConcurrentHashMap<>();
    // Cache of BrokerClients to followers, keyed by "host:port"
    private final Map<String, BrokerClient> followerClients = new ConcurrentHashMap<>();

    private final ExecutorService clientPool;
    private final ObjectMapper mapper = new ObjectMapper();
    private volatile boolean running = false;
    private ServerSocket serverSocket;

   public BrokerServer(int port, int[] ownedPartitions, boolean[] initiallyLeader) {
        this.port = port;
        this.clientPool = Executors.newFixedThreadPool(20);
        for (int i = 0; i < ownedPartitions.length; i++) {
            int p = ownedPartitions[i];
            partitionLogs.put(p, new PartitionLog());
            isLeaderFor.put(p, initiallyLeader[i]);
        }
    }

    /** Call this after construction to declare "I am the LEADER of this partition, replicate to this follower." */
    public void setFollowerFor(int partition, String followerHost, int followerPort) {
        followerFor.put(partition, new ClusterMetadata.BrokerAddress(followerHost, followerPort));
    }

    public void start() throws IOException {
        serverSocket = new ServerSocket(port);
        running = true;
        System.out.println("BrokerServer on port " + port + " owns partitions " + partitionLogs.keySet());

        while (running) {
            Socket clientSocket;
            try {
                clientSocket = serverSocket.accept();
            } catch (IOException e) {
                if (!running) break;
                throw e;
            }
            clientPool.submit(() -> handleClient(clientSocket));
        }
    }

    private void handleClient(Socket socket) {
    try (socket;
         BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
         PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {

        String line;
        while ((line = in.readLine()) != null) {  // loop: handle multiple requests per connection
            Request request = mapper.readValue(line, Request.class);
            Response response = handleRequest(request);
            out.println(mapper.writeValueAsString(response));
        }
        // readLine() returns null when client closes connection - normal, not an error

    } catch (IOException e) {
        if (running) {
            System.err.println("Error handling client: " + e.getMessage());
        }
    }
}
    /**
     * Promotes this broker to LEADER for the given partition. Called when
     * this broker (acting as a follower) detects via HeartbeatMonitor that
     * the current leader is dead.
     *
     * Thread-safety: isLeaderFor is a ConcurrentHashMap, so this write is
     * immediately visible to any client-handler thread checking
     * isLeaderFor.get(partition) in handleRequest - no separate locking
     * needed for this single flag flip.
     */
    public void promoteToLeader(int partition) {
        isLeaderFor.put(partition, true);
        System.out.println(">>> PROMOTED: this broker is now LEADER for partition " + partition);
    }

    private Response handleRequest(Request request) {
        PartitionLog log = partitionLogs.get(request.partition);
        if (log == null) {
            return Response.error("This broker does not own partition " + request.partition);
        }
        try {
            switch (request.type) {
                case "APPEND" -> {
                    return handleAppend(request, log);
                }
                case "FETCH" -> {
                    Response r = Response.ok();
                    r.records = log.fetchFrom(request.fromOffset);
                    return r;
                }
                case "REPLICATE" -> {
                    log.appendAt(request.replicaOffset, request.key, request.value, request.timestamp,
                            request.producerId, request.sequenceNumber);
                    return Response.ok();
                }
                case "HEARTBEAT" -> {
                    return Response.ok();
                }
                default -> {
                    return Response.error("Unknown request type: " + request.type);
                }
            }
        } catch (Exception e) {
            return Response.error("Internal error: " + e.getMessage());
        }
    }

    /**
     * Handles an APPEND on the LEADER: write locally, then replicate to
     * the follower SYNCHRONOUSLY, and only report success to the producer
     * after the follower confirms. This is the actual durability guarantee
     * of the whole system - if this method returns success, the data is
     * provably on two machines, not one.
     */
    private Response handleAppend(Request request, PartitionLog log) {
        Boolean isLeader = isLeaderFor.get(request.partition);
        if (isLeader == null || !isLeader) {
            return Response.error("NOT_LEADER: this broker is not the leader for partition " + request.partition);
        }

        long offset = log.appendIdempotent(request.key, request.value, System.currentTimeMillis(),
                request.producerId, request.sequenceNumber);
        ClusterMetadata.BrokerAddress follower = followerFor.get(request.partition);
        if (follower != null) {
            long actualTimestamp = log.fetchFrom(offset).get(0).getTimestamp();
            BrokerClient followerClient = followerClients.computeIfAbsent(
                    follower.toString(),
                    addr -> new BrokerClient(follower.host, follower.port)
            );
            try {
               Response replicationResponse = followerClient.send(
                        Request.replicate(request.partition, offset, request.key, request.value, actualTimestamp,
                                request.producerId, request.sequenceNumber)
                );
                if (!replicationResponse.success) {
                    return Response.error("Replication to follower failed: " + replicationResponse.error);
                }
            } catch (IOException e) {
                return Response.error("Could not reach follower for replication: " + e.getMessage());
            }
        }

        Response r = Response.ok();
        r.offset = offset;
        return r;
    }
   public boolean isLeaderFor(int partition) {
        Boolean v = isLeaderFor.get(partition);
        return v != null && v;
    }
    public void stop() throws IOException {
        running = false;
        if (serverSocket != null) serverSocket.close();
        clientPool.shutdown();
    }
    private final HeartbeatMonitor heartbeatMonitor = new HeartbeatMonitor();

    public HeartbeatMonitor getHeartbeatMonitor() {
        return heartbeatMonitor;
    }

    public static void main(String[] args) throws IOException {
        // Usage: BrokerServer <port> <partition1:role,partition2:role,...> [followerHost:followerPort:partition ...]
        // role is "L" (leader) or "F" (follower)
        int port = Integer.parseInt(args[0]);
        String[] partStrs = args[1].split(",");
        int[] owned = new int[partStrs.length];
        boolean[] leader = new boolean[partStrs.length];
        for (int i = 0; i < partStrs.length; i++) {
            String[] pieces = partStrs[i].split(":");
            owned[i] = Integer.parseInt(pieces[0]);
            leader[i] = pieces[1].equalsIgnoreCase("L");
        }

        BrokerServer server = new BrokerServer(port, owned, leader);

        for (int i = 2; i < args.length; i++) {
            String[] parts = args[i].split(":");
            server.setFollowerFor(Integer.parseInt(parts[2]), parts[0], Integer.parseInt(parts[1]));
            System.out.println("Partition " + parts[2] + " will replicate to " + parts[0] + ":" + parts[1]);
        }

        server.start();
    }
}