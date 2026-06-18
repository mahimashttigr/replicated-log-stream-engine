package com.yourname.dlog.broker;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * End-to-end test: start a leader + follower broker pair, write continuously
 * through a Producer, kill the leader mid-stream, and assert that every
 * acknowledged write survives with correct, gap-free, duplicate-free
 * ordering once the follower takes over.
 *
 * This is the automated version of the manual three-terminal demo - the
 * single most important piece of evidence for this whole project, because
 * it's repeatable and provable rather than "it worked when I watched it."
 */
class FailoverIntegrationTest {

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void leaderFailure_duringContinuousWrites_resultsInNoDataLossAndCorrectOrdering() throws Exception {

        int leaderPort = 19001;
        int followerPort = 19003;
        int partition = 0;

        // --- Start the leader, replicating partition 0 to the follower ---
        BrokerServer leaderServer = new BrokerServer(leaderPort, new int[]{partition}, new boolean[]{true});
        leaderServer.setFollowerFor(partition, "localhost", followerPort);
        Thread leaderThread = new Thread(() -> {
            try { leaderServer.start(); } catch (Exception ignored) {}
        });
        leaderThread.setDaemon(true);
        leaderThread.start();

        // --- Start the follower, NOT initially leader ---
        BrokerServer followerServer = new BrokerServer(followerPort, new int[]{partition}, new boolean[]{false});
        Thread followerThread = new Thread(() -> {
            try { followerServer.start(); } catch (Exception ignored) {}
        });
        followerThread.setDaemon(true);
        followerThread.start();

        // --- Heartbeat monitor + failover coordinator watching the leader ---
        HeartbeatMonitor monitor = new HeartbeatMonitor();
        monitor.addPeer("leader", "localhost", leaderPort);
        monitor.start();

        FailoverCoordinator coordinator = new FailoverCoordinator(monitor, "leader", followerServer, partition);
        coordinator.start();

        Thread.sleep(500); // let both servers finish binding their sockets before we send traffic

        // --- Producer setup ---
        ClusterMetadata metadata = new ClusterMetadata(1);
        metadata.setLeader(partition, "localhost", leaderPort);
        Producer producer = new Producer(metadata);
        producer.setFailoverTarget(partition, "localhost", followerPort);

        // --- Write continuously, killing the leader partway through ---
        int totalWrites = 200;
        int killAtWrite = 80;
        List<Long> successfulOffsets = new ArrayList<>();
        int failedWrites = 0;

        for (int i = 0; i < totalWrites; i++) {
            if (i == killAtWrite) {
                leaderServer.stop(); // simulates Ctrl+C / crash
            }

            Response r = producer.send("device-X", "reading-" + i);
            if (r.success) {
                successfulOffsets.add(r.offset);
            } else {
                failedWrites++;
            }
        }

        monitor.stop();
        coordinator.stop();

        // --- Assertions: the actual proof ---

        // 1. No more than a small handful of writes should fail during the
        //    brief promotion window - if this is high, failover is too slow.
        assertTrue(failedWrites <= 5,
                "Expected at most a few transient failures during failover, got " + failedWrites);

        // 2. Every successful offset must be unique - no duplicates, even
        //    though some writes were retried across leader -> follower.
        Set<Long> uniqueOffsets = new HashSet<>(successfulOffsets);
        assertEquals(successfulOffsets.size(), uniqueOffsets.size(),
                "Found duplicate offsets among successful writes - idempotency failed");

        // 3. Offsets must be gap-free and contiguous from 0 upward - this is
        //    the core "no data loss, correct ordering" guarantee.
        List<Long> sorted = new ArrayList<>(uniqueOffsets);
        Collections.sort(sorted);
        for (int i = 0; i < sorted.size(); i++) {
            assertEquals((long) i, sorted.get(i),
                    "Gap or ordering violation in offsets at index " + i + ": " + sorted);
        }

        // 4. Directly fetch from the FOLLOWER (now promoted leader) and confirm
        //    its log actually contains every one of those offsets, matching.
        BrokerClient followerClient = new BrokerClient("localhost", followerPort);
        Response fetchResult = followerClient.send(Request.fetch(partition, 0));
        assertTrue(fetchResult.success);
        assertEquals(sorted.size(), fetchResult.records.size(),
                "Follower's final log size doesn't match the number of successful writes");

        System.out.println("Integration test passed: " + successfulOffsets.size() +
                " writes succeeded, " + failedWrites + " transient failures during failover, " +
                "zero data loss, zero duplicates, perfect ordering.");
    }
}