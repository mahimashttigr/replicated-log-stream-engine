package com.yourname.dlog.broker;

public class HeartbeatDemo {
    public static void main(String[] args) throws Exception {
        HeartbeatMonitor monitor = new HeartbeatMonitor();
        monitor.addPeer("broker-9001", "localhost", 9001);
        monitor.start();

        System.out.println("Monitoring localhost:9001. Kill that broker now and watch...");

        for (int i = 0; i < 30; i++) {
            System.out.println("[t=" + i + "s] broker-9001 alive = " + monitor.isAlive("broker-9001"));
            Thread.sleep(1000);
        }

        monitor.stop();
    }
}