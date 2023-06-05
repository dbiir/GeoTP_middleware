package org.apache.shardingsphere.proxy.backend.statistics.monitor;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public final class LockMetaData {
    private double alpha = 0.75;
    private int readCount;
    private int writeCount;
    private double readLatency;
    private double writeLatency;
    private long startTime;

    public LockMetaData() {
        readCount = 0;
        writeCount = 0;
        readLatency = 0;
        writeLatency = 0;
        startTime = System.nanoTime();
    }

    public synchronized void updateReadLatency(double newLatency) {
        readLatency += newLatency;
//        readLatency = readCount == 0 ? newLatency : readLatency * alpha + newLatency * (1 - alpha);
        readCount++;
    }

    public synchronized void updateWriteLatency(double newLatency) {
        writeLatency += newLatency;
//        writeLatency = writeCount == 0 ? newLatency : writeLatency * alpha + newLatency * (1 - alpha);
        writeCount++;
    }

    public String flushMetaData() {
        long endTime = System.nanoTime();
        String result = toString();
        if (result.equals("")) {
            startTime = endTime;
            return result;
        }
        result = "Duration: " + (endTime - startTime) * 1.0 / 1000000 + " ms\t" + result;
        startTime = endTime;
        readCount = 0;
        writeCount = 0;
        readLatency = 0;
        writeLatency = 0;
        return result;
    }

    @Override
    public String toString() {
        if (readCount == 0 && writeCount == 0)
            return "";
        return "{ read_count: " + readCount +
                "\tread_latency: " + readLatency/readCount +
                "\twrite_count: " + writeCount +
                "\twrite_latency: " + writeLatency/writeCount + " }";
    }
}
