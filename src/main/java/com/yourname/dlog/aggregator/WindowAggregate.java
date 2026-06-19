package com.yourname.dlog.aggregator;

public final class WindowAggregate {

    private final long count;
    private final double sum;
    private final double max;

    public WindowAggregate(double firstValue) {
        this.count = 1;
        this.sum = firstValue;
        this.max = firstValue;
    }

    private WindowAggregate(long count, double sum, double max) {
        this.count = count;
        this.sum = sum;
        this.max = max;
    }

    public WindowAggregate combine(WindowAggregate other) {
        return new WindowAggregate(
                this.count + other.count,
                this.sum + other.sum,
                Math.max(this.max, other.max)
        );
    }

    public long getCount() { return count; }
    public double getSum() { return sum; }
    public double getAverage() { return count == 0 ? 0 : sum / count; }
    public double getMax() { return max; }

    @Override
    public String toString() {
        return String.format("count=%d, avg=%.1f, max=%.1f", count, getAverage(), max);
    }
}
