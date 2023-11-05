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

package org.apache.shardingsphere.infra.executor.sql.context;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

/**
 * Execution unit.
 */
@RequiredArgsConstructor
@Getter
@EqualsAndHashCode
@ToString
public final class ExecutionUnit {
    
    private final String dataSourceName;
    
    private final SQLUnit sqlUnit;
    
    private long delayTime;
    
    private int localExecuteLatency; // TODO: millisecond -> microsecond
    
    private double abortProbability = 0;
    
    private int networkLatency;
    
    private int realExecuteLatency;

    private long finishTime;
    
    private boolean isHarp = false;
    
    private final HashMap<String, List<Integer>> keys = new HashMap<>();
    
    public void addKeys(String tableName, Integer key) {
        keys.computeIfAbsent(tableName, unused -> new LinkedList<>()).add(key);
    }
    
    public HashMap<String, List<Integer>> getKeys() {
        return keys;
    }
    
    public List<Integer> findKeysByTableName(String tableName) {
        return keys.get(tableName);
    }
    
    public void updateProbability(double p) {
        abortProbability = 1 - (1 - abortProbability) * p;
    }
    
    public double getAbortProbability() {
        return abortProbability;
    }
    
    /*
     * use for analyse delay time
     */
    public void updateLocalExecuteLatency(int latency) {
        localExecuteLatency += latency;
    }
    
    public void setNetworkLatency(int latency) {
        networkLatency = latency;
    }
    
    public void setRealExecuteLatency(int latency) {
        this.realExecuteLatency = latency;
    }
    
    public void setHarp(boolean isHarp) {
        this.isHarp = isHarp;
    }
    
    public long GetDelayTime() {
        return delayTime;
    }
    
    public void SetDelayTime(long delayTime) {
        this.delayTime = delayTime;
    }

    public void SetFinishTime(long finishTime) { this.finishTime = finishTime; }
    
    public void CombineExecutionUnit(ExecutionUnit other) {
        sqlUnit.CombineSQLUnit(other.getSqlUnit());
    }
}
