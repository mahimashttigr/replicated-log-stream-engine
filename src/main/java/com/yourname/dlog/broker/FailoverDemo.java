package com.yourname.dlog.broker;

public class FailoverDemo {
    public static void main(String[] args) throws Exception {
        // This process plays the FOLLOWER broker for partition 0, port 9003.
        // It starts as a follower (not leader), watches the leader on 9001,
        // and promotes itself if 9001 dies.

        BrokerServer followerServer = new BrokerServer(9003, new int[]{0}, new boolean[]{false});

        HeartbeatMonitor monitor = new HeartbeatMonitor();
        monitor.addPeer("leader-9001", "localhost", 9001);
        monitor.start();

        FailoverCoordinator coordinator = new FailoverCoordinator(monitor, "leader-9001", followerServer, 0);
        coordinator.start();

        System.out.println("Follower broker starting on 9003, watching leader on 9001...");
        followerServer.start(); // blocks
    }
}