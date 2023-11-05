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

package org.apache.shardingsphere.infra.statistics.monitor;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public final class LockMetaData {
    
    private static final int countThreshold = 128;
    private static final double alpha = 0.75;
    
    private int readCount;
    private int writeCount;
    private double readLatency;
    private double writeLatency;
    private long startTime;
    
    // Probabilistic Lock and Feedback
    // TODO: use 8 Byte to store following attributes
    private int count;
    private int successCount;
    private int processing;
    private double latency;
    private double networkThreshold;
    
    public LockMetaData() {
        readCount = 0;
        writeCount = 0;
        count = 1;
        successCount = 1;
        readLatency = 0.01;
        writeLatency = 0.01;
        latency = 0.01;
        startTime = System.nanoTime();
        networkThreshold = 0;
    }
    
    public LockMetaData(double networkLatency) {
        readCount = 0;
        writeCount = 0;
        count = 1;
        successCount = 1;
        readLatency = 0.01;
        writeLatency = 0.01;
        latency = 0.01;
        startTime = System.nanoTime();
        networkThreshold = networkLatency * 2; // hyper-parameter, 1RTT
    }
    
    public synchronized void incCount() {
        count++;
    }
    
    public synchronized void incSuccessCount() {
        successCount++;
    }
    
    /*
     * TODO: change to atomic integer
     */
    public synchronized void incProcessing() {
        processing++;
    }
    
    public synchronized void decProcessing() {
        processing--;
    }
    
    public synchronized double nonBlockProbability() {
        if (count > countThreshold) {
            successCount /= 2;
            count /= 2;
        }
        // TODO:
        // System.out.println("successCount: " + successCount + "; count: " + count + "; processing: " + processing);
        return Math.pow(successCount * 1.0 / count, processing);
    }
    
    public synchronized void updateLatency(double newLatency) {
        latency = (latency * alpha + newLatency * (1 - alpha));
    }
    
    public double getLatency() {
        return latency * (1 - alpha);
    }
    
    public synchronized void updateReadLatency(double newLatency) {
        readLatency += newLatency;
        // readLatency = readCount == 0 ? newLatency : readLatency * alpha + newLatency * (1 - alpha);
        readCount++;
    }
    
    public synchronized void updateWriteLatency(double newLatency) {
        writeLatency += newLatency;
        // writeLatency = writeCount == 0 ? newLatency : writeLatency * alpha + newLatency * (1 - alpha);
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
                "\tread_latency: " + readLatency / readCount +
                "\twrite_count: " + writeCount +
                "\twrite_latency: " + writeLatency / writeCount + " }";
    }
}
