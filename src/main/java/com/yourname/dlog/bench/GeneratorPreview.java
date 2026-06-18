package com.yourname.dlog.bench;

public class GeneratorPreview {
    public static void main(String[] args) {
        EventGenerator generator = new EventGenerator(5, 0.2); // 20% late, exaggerated for visibility in this preview

        int onTimeCount = 0;
        int lateCount = 0;

        for (int i = 0; i < 20; i++) {
            SensorEvent event = generator.generateOne();
            long now = System.currentTimeMillis();
            boolean isLate = (now - event.getTimestamp()) > 500; // more than half a second old = "late" for this check
            System.out.println(event + (isLate ? "   <-- LATE" : ""));
            if (isLate) lateCount++; else onTimeCount++;
        }

        System.out.println("\nOn-time: " + onTimeCount + ", Late: " + lateCount);
    }
}