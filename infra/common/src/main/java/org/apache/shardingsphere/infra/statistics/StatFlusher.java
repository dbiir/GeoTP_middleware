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

package org.apache.shardingsphere.infra.statistics;

import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import org.apache.shardingsphere.infra.statistics.monitor.LocalLockTable;
import org.apache.shardingsphere.infra.statistics.network.Latency;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

@NoArgsConstructor
public final class StatFlusher implements Runnable {
    
    private SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmm");
    private String logPath = "logs/";
    private List<String> tables = new LinkedList<>();
    private final int batchSize = 128;
    
    @SneakyThrows
    @Override
    public void run() {
        LocalLockTable localLockTable = LocalLockTable.getInstance();
        String outputLockFilePath = logPath + "lock_" + df.format(new Date()) + ".log";
        String outputNetworkFilePath = logPath + "network_" + df.format(new Date()) + ".log";
        // TODO: flush dynamic network info
        
        while (true) {
            Thread.sleep(50);
            StringBuilder stats = new StringBuilder();
            StringBuilder statsNetwork = new StringBuilder();
            
            try {
                long startTime = System.currentTimeMillis();
                
                for (String each : localLockTable.getTableNameToLockMetaData().keySet()) {
                    stats = new StringBuilder("Table name: " + each + "\n");
                    int entrySize = localLockTable.getTableNameToLockMetaData().get(each).length;
                    for (int i = 0; i < entrySize; i++) {
                        String str = localLockTable.getLockMetaData(each, i).flushMetaData();
                        if (!Objects.equals(str, "")) {
                            stats.append("i: ").append(i).append("\t").append(str).append("\n");
                        }
                        
                        if ((i + 1) % batchSize == 0) {
                            outputToFile(outputLockFilePath, stats.toString());
                            stats = new StringBuilder();
                        }
                    }
                }
                
                stats.append("Consume of flushing: ").append(System.currentTimeMillis() - startTime).append(" ms\n");
                statsNetwork.append(Latency.getInstance().toString());
                outputToFile(outputLockFilePath, stats.toString());
                outputToFile(outputNetworkFilePath, statsNetwork.toString());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
    
    private void outputToFile(String filePath, String exportedData) throws IOException {
        File file = new File(filePath);
        if (!file.exists()) {
            file.getParentFile().mkdirs();
            if (!file.createNewFile()) {
                throw new IOException();
            }
        }
        
        try (FileOutputStream output = new FileOutputStream(file, true)) {
            output.write(exportedData.getBytes());
            output.flush();
        } catch (final IOException ex) {
            throw ex;
        }
    }
    
}
