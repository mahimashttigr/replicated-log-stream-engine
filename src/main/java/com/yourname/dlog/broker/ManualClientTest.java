package com.yourname.dlog.broker;

public class ManualClientTest {
    public static void main(String[] args) throws Exception {
        ClusterMetadata metadata = new ClusterMetadata(1);
        metadata.setLeader(0, "localhost", 9001);

        Producer producer = new Producer(metadata);
        producer.setFailoverTarget(0, "localhost", 9003);

        System.out.println("Writing continuously. Kill the broker on port 9001 partway through (Ctrl+C in its terminal).");

        for (int i = 0; i < 30; i++) {
            try {
                Response r = producer.send("device-X", "reading-" + i);
                System.out.println("write " + i + " -> success=" + r.success +
                        (r.success ? " offset=" + r.offset : " error=" + r.error));
            } catch (Exception e) {
                System.out.println("write " + i + " -> EXCEPTION: " + e.getMessage());
            }
            Thread.sleep(1000);
        }
    }
}