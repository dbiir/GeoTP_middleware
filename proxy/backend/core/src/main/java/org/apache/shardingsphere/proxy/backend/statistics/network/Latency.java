package org.apache.shardingsphere.proxy.backend.statistics.network;

import lombok.Getter;

import java.util.HashMap;

public final class Latency {
    private static final Latency INSTANCE = new Latency(0.8, 20);
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
        latencies = new HashMap<String, double[]>() {};
        srcToIp = new HashMap<String, String>() {};
    }

    public void AddDataSource(String src) {
        latencies.put(src, new double[windowSize+1]);
        // TODO: hard code
        latencies.get(src)[windowSize] = 0;
    }

    public void SetDataSourceIp(String src, String ip) {
        if (!srcToIp.containsKey(src)) {
            srcToIp.put(src, ip);
        }
    }

    public double GetLatency(String src) {
        if (latencies.containsKey(src)) {
            return latencies.get(src)[windowSize];
        } else {
            return -1.0;
        }
    }

    public void UpdateLatency(String src, double latency, int count) {
        latencies.get(src)[count] = alpha * latencies.get(src)[count] + (1 - alpha) * latencies.get(src)[count];
        latencies.get(src)[windowSize] = latencies.get(src)[count];
    }

    public double GetStableLatency(String src) {
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

    public void SetAlgorithm(String alg) {
        this.algorithm = alg.toLowerCase();
    }

    public boolean NeedDelay() {
        return algorithm.contains("harp");
    }

    public static Latency getInstance() {
        return INSTANCE;
    }
}
