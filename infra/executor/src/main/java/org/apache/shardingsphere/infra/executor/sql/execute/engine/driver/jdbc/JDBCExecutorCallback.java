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

package org.apache.shardingsphere.infra.executor.sql.execute.engine.driver.jdbc;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.apache.shardingsphere.infra.database.metadata.DataSourceMetaData;
import org.apache.shardingsphere.infra.database.type.DatabaseType;
import org.apache.shardingsphere.infra.executor.kernel.model.ExecutorCallback;
import org.apache.shardingsphere.infra.executor.sql.context.ExecutionUnit;
import org.apache.shardingsphere.infra.executor.sql.context.SQLUnit;
import org.apache.shardingsphere.infra.executor.sql.execute.engine.ConnectionMode;
import org.apache.shardingsphere.infra.executor.sql.execute.engine.SQLExecutionUnit;
import org.apache.shardingsphere.infra.executor.sql.execute.engine.SQLExecutorExceptionHandler;
import org.apache.shardingsphere.infra.executor.sql.hook.SPISQLExecutionHook;
import org.apache.shardingsphere.infra.executor.sql.hook.SQLExecutionHook;
import org.apache.shardingsphere.infra.executor.sql.process.ExecuteProcessEngine;
import org.apache.shardingsphere.sql.parser.sql.common.statement.SQLStatement;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * JDBC executor callback.
 *
 * @param <T> class type of return value
 */
@RequiredArgsConstructor
public abstract class JDBCExecutorCallback<T> implements ExecutorCallback<JDBCExecutionUnit, T> {
    
    private static final Map<String, DataSourceMetaData> CACHED_DATASOURCE_METADATA = new ConcurrentHashMap<>();
    
    private final DatabaseType protocolType;
    
    private final Map<String, DatabaseType> storageTypes;
    
    private final SQLStatement sqlStatement;
    
    private final boolean isExceptionThrown;
    
    @Override
    public final Collection<T> execute(final Collection<JDBCExecutionUnit> executionUnits, final boolean isTrunkThread) throws SQLException {
        // TODO It is better to judge whether need sane result before execute, can avoid exception thrown
        Collection<T> result = new LinkedList<>();
        long startTime = System.nanoTime();
        for (JDBCExecutionUnit each : executionUnits) {
            T executeResult = execute(each, isTrunkThread);
            if (null != executeResult) {
                result.add(executeResult);
            }
        }
        System.out.println("JDBCExecutorCallback execution time: " + (System.nanoTime() - startTime) / 1000000 + " ms");
        return result;
    }
    
    /*
     * To make sure SkyWalking will be available at the next release of ShardingSphere, a new plugin should be provided to SkyWalking project if this API changed.
     *
     * @see <a href="https://github.com/apache/skywalking/blob/master/docs/en/guides/Java-Plugin-Development-Guide.md#user-content-plugin-development-guide">Plugin Development Guide</a>
     */
    @SneakyThrows
    private T execute(final JDBCExecutionUnit jdbcExecutionUnit, final boolean isTrunkThread) throws SQLException {
        long start = System.nanoTime();
        SQLExecutorExceptionHandler.setExceptionThrown(isExceptionThrown);
        DatabaseType storageType = storageTypes.get(jdbcExecutionUnit.getExecutionUnit().getDataSourceName());
        DataSourceMetaData dataSourceMetaData = getDataSourceMetaData(jdbcExecutionUnit.getStorageResource().getConnection().getMetaData(), storageType);
        SQLExecutionHook sqlExecutionHook = new SPISQLExecutionHook();
        try {
            ExecutionUnit executionUnit = jdbcExecutionUnit.getExecutionUnit();
            // delay execution
            if (executionUnit.GetDelayTime() > 0) {
                Thread.sleep(executionUnit.GetDelayTime());
                // Thread.sleep(140);
                // System.out.println("data source: " + executionUnit.getDataSourceName() + " | delay: " + jdbcExecutionUnit.getExecutionUnit().GetDelayTime() + "ms");
            }
            SQLUnit sqlUnit = executionUnit.getSqlUnit();
            sqlExecutionHook.start(jdbcExecutionUnit.getExecutionUnit().getDataSourceName(), sqlUnit.getSql(), sqlUnit.getParameters(), dataSourceMetaData, isTrunkThread);
            long startTime = System.nanoTime();
            T result = executeSQL(sqlUnit.getSql(), jdbcExecutionUnit.getStorageResource(), jdbcExecutionUnit.getConnectionMode(), storageType);
            // if (executionUnit.getDataSourceName().equals("ds_1")) {
            // Thread.sleep(100);
            // } else if (executionUnit.getDataSourceName().equals("ds_0")) {
            // Thread.sleep(20);
            // }
            executionUnit.SetFinishTime(System.nanoTime() / 1000000);
            long executeTime = System.nanoTime() - startTime;
            executionUnit.setRealExecuteLatency((int) (executeTime / 1000000));
            sqlExecutionHook.finishSuccess();
            finishReport(jdbcExecutionUnit);
            System.out.println("execute time: " + (System.nanoTime() - start) / 1000000 + " ms");
            return result;
        } catch (final SQLException ex) {
            if (!storageType.equals(protocolType)) {
                Optional<T> saneResult = getSaneResult(sqlStatement, ex);
                if (saneResult.isPresent()) {
                    return isTrunkThread ? saneResult.get() : null;
                }
            }
            sqlExecutionHook.finishFailure(ex);
            SQLExecutorExceptionHandler.handleException(ex);
            return null;
        }
    }
    
    private DataSourceMetaData getDataSourceMetaData(final DatabaseMetaData databaseMetaData, final DatabaseType storageType) throws SQLException {
        String url = databaseMetaData.getURL();
        if (CACHED_DATASOURCE_METADATA.containsKey(url)) {
            return CACHED_DATASOURCE_METADATA.get(url);
        }
        DataSourceMetaData result = storageType.getDataSourceMetaData(url, databaseMetaData.getUserName());
        CACHED_DATASOURCE_METADATA.put(url, result);
        return result;
    }
    
    private void finishReport(final SQLExecutionUnit executionUnit) {
        new ExecuteProcessEngine().finishExecution(executionUnit);
    }
    
    protected abstract T executeSQL(String sql, Statement statement, ConnectionMode connectionMode, DatabaseType storageType) throws SQLException;
    
    protected abstract Optional<T> getSaneResult(SQLStatement sqlStatement, SQLException ex);
}
