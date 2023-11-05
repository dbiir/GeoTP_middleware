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

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public final class LatencyCollector implements Runnable {
    
    @Override
    public void run() {
        int windowSize = Latency.getInstance().getWindowSize();
        int count = 0;
        
        while (true) {
            for (Map.Entry<String, String> each : Latency.getInstance().getSrcToIp().entrySet()) {
                try {
                    Latency.getInstance().UpdateLatency(each.getKey(), getLatency(each.getValue()).latency, count);
                } catch (Exception e) {
                    System.out.println(e);
                }
            }
            count = (count + 1) % windowSize;
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        
    }
    
    private static final class PingMessage {
        
        private double packetLoss;
        private double latency;
        
        public PingMessage(double packetLoss, double latency) {
            this.packetLoss = packetLoss;
            this.latency = latency;
        }
        
        public PingMessage() {
        }
        
        public void setLatency(double latency) {
            this.latency = latency;
        }
        
        public void setPacketLoss(double packetLoss) {
            this.packetLoss = packetLoss;
        }
    }
    
    public PingMessage getLatency(String ip) throws Exception {
        BufferedReader reader = null;
        try {
            PingMessage pingMessage = new PingMessage();
            Runtime runtime = Runtime.getRuntime();
            Process process = runtime.exec("sudo ping " + ip + " -i 0.001 -c 1");
            InputStreamReader inputStreamReader = new InputStreamReader(process.getInputStream(), "GB2312");
            reader = new BufferedReader(inputStreamReader);
            String line;
            StringBuilder buffer = new StringBuilder();
            
            while ((line = reader.readLine()) != null) {
                buffer.append(line);
                if (line.contains("packet loss")) {
                    String pl = Arrays.stream(line.split(",")).filter((each) -> each.contains("packet loss"))
                            .collect(Collectors.toList()).get(0).trim().split(" ")[0];
                    pingMessage.setPacketLoss(Double.parseDouble(pl.substring(0, pl.length() - 1)));
                } else if (line.contains("rtt")) {
                    String[] strings = line.split(" ");
                    assert (strings.length == 5);
                    pingMessage.setLatency(Double.parseDouble(strings[3].split("/")[2]));
                }
            }
            
            return pingMessage;
        } catch (Exception e) {
            throw new Exception();
        } finally {
            if (reader != null) {
                reader.close();
            }
        }
    }
}
