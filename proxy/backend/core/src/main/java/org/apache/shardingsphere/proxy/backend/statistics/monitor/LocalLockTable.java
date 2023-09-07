/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.shardingsphere.proxy.backend.statistics.monitor;

import lombok.Getter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

@Getter
public final class LocalLockTable {
    
    private static final LocalLockTable INSTANCE;
    
    static {
        try {
            INSTANCE = new LocalLockTable(100001);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    private boolean enableStatistic;
    private static final double alpha = 0.875;
    private final int totalNum;
    private final ConcurrentHashMap<String, LockMetaData[]> tableNameToLockMetaData = new ConcurrentHashMap<>(15);
    private final HashMap<String, Integer> DefaultTableNameToSize = new HashMap<>(10);
    String fileName = "default_table_size";
    private LocalLockTable(int totalNum) throws IOException {
        this.enableStatistic = false;
        this.totalNum = totalNum;
        // tableNameToLockMetaData.put("usertable", new LockMetaData[totalNum]);
        // for (int i = 0; i < totalNum; i++) {
        // tableNameToLockMetaData.get("usertable")[i] = new LockMetaData();
        // }
        List<String> lines = Files.readAllLines(Paths.get(fileName), StandardCharsets.UTF_8);
        
        for (String each : lines) {
            String[] results = each.split(" ");
            System.out.println(results[0] + " " + results[1]);
            DefaultTableNameToSize.put(results[0], Integer.valueOf(results[1]));
            registerTable(results[0], Integer.parseInt(results[1]));
        }
    }
    
    // TODO: (urgency level: low) add LRU strategy to this Array;
    public void registerTable(String tableName, int tableScale) {
        if (tableNameToLockMetaData.get(tableName) == null) {
            System.out.println("register " + tableName + ", Size " + tableScale);
            tableNameToLockMetaData.put(tableName, new LockMetaData[tableScale]);
            for (int i = 0; i < tableScale; i++) {
                tableNameToLockMetaData.get(tableName)[i] = new LockMetaData();
            }
        }
    }
    
    public void registerTable(String tableName) {
        if (tableNameToLockMetaData.get(tableName) == null) {
            int size = getDefaultTableSize(tableName);
            System.out.println("register " + tableName + ", Size " + size);
            tableNameToLockMetaData.put(tableName, new LockMetaData[size]);
            for (int i = 0; i < totalNum; i++) {
                tableNameToLockMetaData.get(tableName)[i] = new LockMetaData();
            }
        }
    }
    
    private int getDefaultTableSize(String tableName) {
        if (DefaultTableNameToSize.get(tableName) != null) {
            return DefaultTableNameToSize.get(tableName);
        }
        return totalNum;
    }
    
    public void updateLockTime(String tableName, int[] entryIndexes, double[] latencies, boolean[] ops) {
        int len = entryIndexes.length;
        
        for (int i = 0; i < len; i++) {
            if (ops[i]) {
                // write op if equals 1
                tableNameToLockMetaData.get(tableName)[entryIndexes[i]]
                        .updateReadLatency(latencies[i]);
            } else {
                tableNameToLockMetaData.get(tableName)[entryIndexes[i]]
                        .updateWriteLatency(latencies[i]);
            }
        }
    }
    
    public void updateLockTime(String tableName, int entryIndex, double latency, boolean ops) {
        if (ops) {
            // write op if equals 1
            tableNameToLockMetaData.get(tableName)[entryIndex]
                    .updateReadLatency(latency);
        } else {
            tableNameToLockMetaData.get(tableName)[entryIndex]
                    .updateWriteLatency(latency);
        }
    }
    
    public LockMetaData[] getLockMetaData(String tableName) {
        return tableNameToLockMetaData.get(tableName);
    }
    
    public LockMetaData getLockMetaData(String tableName, int index) {
        if (tableNameToLockMetaData.get(tableName) == null || index < 0)
            return null;
        
        return tableNameToLockMetaData.get(tableName)[index];
    }
    
    public void setEnableStatistical(boolean stat) {
        this.enableStatistic = stat;
    }
    
    public boolean needStatistic() {
        return this.enableStatistic;
    }
    
    public static LocalLockTable getInstance() {
        return INSTANCE;
    }
    
}
