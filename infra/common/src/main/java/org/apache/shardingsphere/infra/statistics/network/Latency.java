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

package org.apache.shardingsphere.infra.statistics.network;

import lombok.Getter;

import java.util.HashMap;
import java.util.Map;

public final class Latency {
    
    private static final Latency INSTANCE = new Latency(0.8, 20);
    @Getter
    private String algorithm;
    private final HashMap<String, double[]> latencies;
    @Getter
    private final HashMap<String, String> srcToIp;
    @Getter
    private final int windowSize;
    private final double alpha;
    
    public Latency(double alpha, int windowSize) {
        this.algorithm = "";
        this.alpha = alpha;
        this.windowSize = windowSize;
        latencies = new HashMap<String, double[]>() {
        };
        srcToIp = new HashMap<String, String>() {
        };
    }
    
    public void AddDataSource(String src) {
        System.out.println("Add Source " + src);
        latencies.put(src, new double[windowSize + 1]);
        // if (src.contains("ds_1")) {
        // latencies.get(src)[windowSize] = 150;
        // } else {
        // latencies.get(src)[windowSize] = 20;
        // }
    }
    
    public void SetDataSourceIp(String src, String ip) {
        if (!srcToIp.containsKey(src)) {
            srcToIp.put(src, ip);
        }
    }
    
    public boolean IsPostgreSQLNode(String resource_name) {
        for (Map.Entry<String, String> each : srcToIp.entrySet()) {
            if (resource_name.contains(each.getKey())) {
                // find the ip of postgreSQL node
                if (each.getValue().equals("10.77.70.89") || each.getValue().equals("10.77.70.90") || each.getValue().equals("127.0.0.1")) {
                    return true;
                }
            }
        }
        return false;
    }
    
    public double GetLatency(String src) {
        if (latencies.containsKey(src)) {
            return latencies.get(src)[windowSize];
        } else {
            return -1.0;
        }
    }
    
    public void UpdateLatency(String src, double latency, int count) {
        latencies.get(src)[count] = latency;
        latencies.get(src)[windowSize] = alpha * latencies.get(src)[windowSize] + (1 - alpha) * latency;
    }
    
    public double getStableLatency(String src) {
        return latencies.get(src)[windowSize];
    }
    
    public double GetMaxLatency(String src) {
        double lat = 0;
        if (latencies.containsKey(src)) {
            double[] tmp = latencies.get(src);
            for (int i = 0; i < windowSize; i++) {
                lat = Math.max(lat, tmp[i]);
            }
            return lat;
        } else {
            return -1.0;
        }
    }
    
    public double getLongestLatency() {
        double longest = 0.0;
        for (String each : latencies.keySet()) {
            longest = Math.max(longest, getStableLatency(each));
        }
        return longest * 2;
    }
    
    public void SetAlgorithm(String alg) {
        this.algorithm = alg.toLowerCase();
    }
    
    public boolean NeedDelay() {
        return algorithm.equals("harp") || algorithm.equals("aharp") ||
                algorithm.equals("harp_lp") || algorithm.equals("aharp_lp") ||
                algorithm.equals("harp_pa") || algorithm.equals("aharp_pa") ||
                algorithm.equals("harp_lppa") || algorithm.equals("aharp_lppa");
    }
    
    public boolean NeedLatencyPredict() {
        return algorithm.equals("harp_lp") || algorithm.equals("aharp_lp");
    }
    
    public boolean NeedPreAbort() {
        return algorithm.equals("harp_pa") || algorithm.equals("aharp_pa");
    }
    
    public boolean NeedLatencyPredictionAndPreAbort() {
        return algorithm.equals("harp_lppa") || algorithm.equals("aharp_lppa");
    }
    
    public boolean asyncPreparation() {
        return algorithm.equals("aharp") || algorithm.equals("aharp_lp") || algorithm.equals("aharp_pa") ||
                algorithm.equals("aharp_lppa") || algorithm.equals("a");
    }
    
    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        for (Map.Entry<String, String> each : srcToIp.entrySet()) {
            result.append(each.getKey()).append("- {ip: ").append(each.getValue()).append("latency: ").append(getStableLatency(each.getKey())).append(" ms}\n");
        }
        return result.toString();
    }
    
    public static Latency getInstance() {
        return INSTANCE;
    }
}
