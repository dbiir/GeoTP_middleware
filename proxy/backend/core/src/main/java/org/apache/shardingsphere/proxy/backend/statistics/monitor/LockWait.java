package org.apache.shardingsphere.proxy.backend.statistics.monitor;

import lombok.Getter;

import java.util.concurrent.ConcurrentHashMap;

@Getter
public final class LockWait {
    private static final LockWait INSTANCE = new LockWait(100001);
    private boolean enableStatistic;
    private static final double alpha = 0.875;
    private final int totalNum;
    private final ConcurrentHashMap<String, LockMetaData[]> tableToLockTimes = new ConcurrentHashMap<>(15);
    private LockWait(int totalNum) {
        this.enableStatistic = false;
        this.totalNum = totalNum;
        tableToLockTimes.put("usertable", new LockMetaData[totalNum]);
        for (int i = 0; i < totalNum; i++) {
            tableToLockTimes.get("usertable")[i] = new LockMetaData();
        }
    }

    // TODO: (urgency level: low) add LRU strategy to this Array;
    public void registerTable(String tableName, int tableScale) {
        if (tableToLockTimes.get(tableName) == null) {
            tableToLockTimes.put(tableName, new LockMetaData[tableScale]);
            for (int i = 0; i < tableScale; i++) {
                tableToLockTimes.get(tableName)[i] = new LockMetaData();
            }
        }
    }

    public void registerTable(String tableName) {
        if (tableToLockTimes.get(tableName) == null) {
            tableToLockTimes.put(tableName, new LockMetaData[totalNum]);
            for (int i = 0; i < totalNum; i++) {
                tableToLockTimes.get(tableName)[i] = new LockMetaData();
            }
        }
    }

    public void updateLockTime(String tableName, int[] entryIndexes, double[] latencies, boolean[] ops) {
        int len = entryIndexes.length;

        for (int i = 0; i < len; i++) {
            if (ops[i]) {
                // write op if equals 1
                tableToLockTimes.get(tableName)[entryIndexes[i]]
                        .updateReadLatency(latencies[i]);
            } else {
                tableToLockTimes.get(tableName)[entryIndexes[i]]
                        .updateWriteLatency(latencies[i]);
            }
        }
    }

    public void updateLockTime(String tableName, int entryIndex, double latency, boolean ops) {
        if (ops) {
            // write op if equals 1
            tableToLockTimes.get(tableName)[entryIndex]
                    .updateReadLatency(latency);
        } else {
            tableToLockTimes.get(tableName)[entryIndex]
                    .updateWriteLatency(latency);
        }
    }

    public LockMetaData[] getLockTimes(String tableName) {
        return tableToLockTimes.get(tableName);
    }

    public LockMetaData getLockTime(String tableName, int index) {
        return tableToLockTimes.get(tableName)[index];
    }

    public void setEnableStatistical(boolean stat) {
        this.enableStatistic = stat;
    }

    public boolean needStatistic() {
        return this.enableStatistic;
    }

    public static LockWait getInstance() {
        return INSTANCE;
    }

}
