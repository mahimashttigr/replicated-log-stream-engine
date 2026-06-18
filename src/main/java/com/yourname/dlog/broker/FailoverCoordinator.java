package com.yourname.dlog.broker;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Watches a HeartbeatMonitor for a specific leader peer; if that leader
 * is detected dead AND this broker is the designated backup for the given
 * partition, promotes this broker's BrokerServer to leader for that
 * partition.
 *
 * This runs as its own periodic check (every 500ms) on its own thread via
 * ScheduledExecutorService - intentionally decoupled from the heartbeat
 * ticking itself, so the "detect" concern and the "react" concern stay
 * separate and each easy to reason about on its own.
 */
public class FailoverCoordinator {

    private final HeartbeatMonitor monitor;
    private final String leaderPeerName;
    private final BrokerServer myBrokerServer;
    private final int partition;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private volatile boolean alreadyPromoted = false;

    public FailoverCoordinator(HeartbeatMonitor monitor, String leaderPeerName,
                                 BrokerServer myBrokerServer, int partition) {
        this.monitor = monitor;
        this.leaderPeerName = leaderPeerName;
        this.myBrokerServer = myBrokerServer;
        this.partition = partition;
    }

    public void start() {
        scheduler.scheduleAtFixedRate(this::checkAndMaybePromote, 0, 500, TimeUnit.MILLISECONDS);
    }

    private void checkAndMaybePromote() {
        if (alreadyPromoted) return;
        if (!monitor.isAlive(leaderPeerName)) {
            System.out.println(">>> Detected leader '" + leaderPeerName + "' is DEAD. Promoting self for partition " + partition + "...");
            myBrokerServer.promoteToLeader(partition);
            alreadyPromoted = true;
        }
    }

    public void stop() {
        scheduler.shutdownNow();
    }
}