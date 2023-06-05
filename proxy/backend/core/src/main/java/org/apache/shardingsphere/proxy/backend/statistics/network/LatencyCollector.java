package org.apache.shardingsphere.proxy.backend.statistics.network;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public final class LatencyCollector implements Runnable{
    @Override
    public void run() {
        int windowSize = Latency.getInstance().getWindowSize();
        int count = 0;

        while (true) {
            for (Map.Entry<String, String> each: Latency.getInstance().getSrcToIp().entrySet()) {
                try {
                    Latency.getInstance().UpdateLatency(each.getKey(), getLatency(each.getValue()).latency, count);
                } catch (Exception e) {
                    System.out.println(e);
                }
            }
            count++;
        }
    }

    private static final class PingMessage {
        private double packetLoss;
        private double latency;

        public PingMessage(double packetLoss, double latency) {
            this.packetLoss = packetLoss;
            this.latency = latency;
        }

        public PingMessage() {}

        public void setLatency(double latency) {
            this.latency = latency;
        }

        public void setPacketLoss(double packetLoss) {
            this.packetLoss = packetLoss;
        }
    }

    public PingMessage getLatency(String ip) throws Exception {
        BufferedReader reader = null;
        try{
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
                            .collect(Collectors.toList()).get(0).split(" ")[0];
                    pingMessage.setPacketLoss(Double.parseDouble(pl));
                } else if (line.contains("rtt")) {
                    String[] strings = line.split(" ");
                    assert (strings.length == 5);
                    pingMessage.setLatency(Double.parseDouble(strings[3].split("/")[2]));
                }
            }

            return pingMessage;
        }catch (Exception e){
            throw new Exception();
        }finally {
            if (reader != null){
                reader.close();
            }
        }
    }
}
