package com.yourname.dlog.broker;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Periodically pings a fixed set of peer brokers ("HEARTBEAT" requests)
 * and tracks which ones are currently considered alive vs. dead.
 *
 * Concurrency design: lastSuccessfulPing is a ConcurrentHashMap because
 * it's WRITTEN by the scheduled heartbeat thread (every tick, for every
 * peer) and READ by isAlive(), which may be called from other threads
 * (e.g. failover logic in Day 6, or just a status-check endpoint). A plain
 * HashMap would not guarantee that a reading thread ever sees the latest
 * write from the heartbeat thread - ConcurrentHashMap gives us both
 * thread-safe mutation AND guaranteed visibility of updates across threads.
 *
 * We mark a peer DEAD after missing MAX_MISSED_HEARTBEATS consecutive
 * pings, not just one - a single slow/dropped ping over a network
 * shouldn't immediately declare a healthy broker dead.
 */
public class HeartbeatMonitor {

    private static final long HEARTBEAT_INTERVAL_MS = 1000;
    private static final int MAX_MISSED_HEARTBEATS = 3;

    private final Map<String, ClusterMetadata.BrokerAddress> peers = new ConcurrentHashMap<>();
    private final Map<String, Long> lastSuccessfulPing = new ConcurrentHashMap<>();
    private final Map<String, BrokerClient> clients = new ConcurrentHashMap<>();

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    public void addPeer(String name, String host, int port) {
        peers.put(name, new ClusterMetadata.BrokerAddress(host, port));
        clients.put(name, new BrokerClient(host, port));
        lastSuccessfulPing.put(name, System.currentTimeMillis()); // assume alive at startup
    }

    public void start() {
        scheduler.scheduleAtFixedRate(this::pingAllPeers, 0, HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    private void pingAllPeers() {
        for (String name : peers.keySet()) {
            BrokerClient client = clients.get(name);
            try {
                Response r = client.send(Request.heartbeat());
                if (r.success) {
                    lastSuccessfulPing.put(name, System.currentTimeMillis());
                }
            } catch (Exception e) {
                // Ping failed (connection refused, timeout, etc.) - we simply
                // don't update lastSuccessfulPing, so isAlive() will notice
                // this peer is falling behind on the next check.
            }
        }
    }

    /**
     * A peer is alive if its most recent successful ping happened within
     * MAX_MISSED_HEARTBEATS * HEARTBEAT_INTERVAL_MS of now. This is a pure
     * read of shared state, safe to call from any thread.
     */
    public boolean isAlive(String name) {
        Long last = lastSuccessfulPing.get(name);
        if (last == null) return false;
        long allowedSilence = MAX_MISSED_HEARTBEATS * HEARTBEAT_INTERVAL_MS;
        return (System.currentTimeMillis() - last) <= allowedSilence;
    }

    public void stop() {
        scheduler.shutdownNow();
    }
}