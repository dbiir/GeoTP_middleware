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

package org.apache.shardingsphere.proxy;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.shardingsphere.infra.config.props.ConfigurationProperties;
import org.apache.shardingsphere.infra.config.props.ConfigurationPropertyKey;
import org.apache.shardingsphere.proxy.arguments.BootstrapArguments;
import org.apache.shardingsphere.proxy.backend.config.ProxyConfigurationLoader;
import org.apache.shardingsphere.proxy.backend.config.YamlProxyConfiguration;
import org.apache.shardingsphere.proxy.backend.config.yaml.YamlProxyDataSourceConfiguration;
import org.apache.shardingsphere.proxy.backend.config.yaml.YamlProxyDatabaseConfiguration;
import org.apache.shardingsphere.infra.statistics.StatFlusher;
import org.apache.shardingsphere.infra.statistics.monitor.LocalLockTable;
import org.apache.shardingsphere.infra.statistics.network.Latency;
import org.apache.shardingsphere.infra.statistics.network.LatencyCollector;
import org.apache.shardingsphere.proxy.frontend.CDCServer;
import org.apache.shardingsphere.proxy.frontend.ShardingSphereProxy;
import org.apache.shardingsphere.proxy.initializer.BootstrapInitializer;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * ShardingSphere-Proxy Bootstrap.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class Bootstrap {
    
    /**
     * Main entrance.
     * 
     * @param args startup arguments
     * @throws IOException IO exception
     * @throws SQLException SQL exception
     */
    public static void main(final String[] args) throws IOException, SQLException {
        BootstrapArguments bootstrapArgs = new BootstrapArguments(preProcessingArgs(args));
        YamlProxyConfiguration yamlConfig = ProxyConfigurationLoader.load(bootstrapArgs.getConfigurationPath());
        int port = bootstrapArgs.getPort().orElseGet(() -> new ConfigurationProperties(yamlConfig.getServerConfiguration().getProps()).getValue(ConfigurationPropertyKey.PROXY_DEFAULT_PORT));
        List<String> addresses = bootstrapArgs.getAddresses();
        new BootstrapInitializer().init(yamlConfig, port, bootstrapArgs.getForce());
        Optional.ofNullable((Integer) yamlConfig.getServerConfiguration().getProps().get(ConfigurationPropertyKey.CDC_SERVER_PORT.getKey()))
                .ifPresent(cdcPort -> new CDCServer(addresses, cdcPort).start());
        // initial state of harp
        initialStateOfHarp(yamlConfig);
        
        ShardingSphereProxy shardingSphereProxy = new ShardingSphereProxy();
        bootstrapArgs.getSocketPath().ifPresent(shardingSphereProxy::start);
        shardingSphereProxy.start(port, addresses);
    }
    
    private static void initialStateOfHarp(YamlProxyConfiguration yamlConfig) {
        for (Map.Entry<String, YamlProxyDatabaseConfiguration> each : yamlConfig.getDatabaseConfigurations().entrySet()) {
            if (each.getValue() != null) {
                for (Map.Entry<String, YamlProxyDataSourceConfiguration> ds : each.getValue().getDataSources().entrySet()) {
                    Latency.getInstance().AddDataSource(ds.getKey());
                    String regEx = "((2[0-4]\\d|25[0-5]|[01]?\\d\\d?)\\.){3}(2[0-4]\\d|25[0-5]|[01]?\\d\\d?)";
                    Pattern p = Pattern.compile(regEx);
                    Matcher m = p.matcher(ds.getValue().getUrl());
                    if (m.find()) {
                        Latency.getInstance().SetDataSourceIp(ds.getKey(), m.group());
                        System.out.println(m.group());
                    }
                }
            }
        }
        
        if (LocalLockTable.getInstance().needStatistic()) {
            Thread flushThread = new Thread(new StatFlusher());
            flushThread.start();
        }
        
        if (Latency.getInstance().NeedDelay()) {
            System.out.println("----- start ping thread -----");
            Thread pingThread = new Thread(new LatencyCollector());
            pingThread.start();
        }
    }
    
    private static String[] preProcessingArgs(final String[] args) {
        List<String> result = new LinkedList<>();
        for (String each : args) {
            if (each.contains("--stat")) {
                LocalLockTable.getInstance().setEnableStatistical(each.contains("true"));
            } else if (each.contains("--alg")) {
                String[] split = each.split("=");
                Latency.getInstance().SetAlgorithm(split[split.length - 1]);
            } else {
                result.add(each);
            }
        }
        return result.toArray(new String[0]);
    }
}
