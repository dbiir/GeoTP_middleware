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

package org.apache.shardingsphere.proxy.frontend.mysql.command.query.text.query;

import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.shardingsphere.infra.binder.QueryContext;
import org.apache.shardingsphere.infra.binder.SQLStatementContextFactory;
import org.apache.shardingsphere.infra.binder.statement.SQLStatementContext;
import org.apache.shardingsphere.infra.binder.statement.dml.SelectStatementContext;
import org.apache.shardingsphere.infra.binder.statement.dml.UpdateStatementContext;
import org.apache.shardingsphere.infra.config.props.ConfigurationPropertyKey;
import org.apache.shardingsphere.infra.context.kernel.KernelProcessor;
import org.apache.shardingsphere.infra.database.type.DatabaseType;
import org.apache.shardingsphere.infra.executor.audit.SQLAuditEngine;
import org.apache.shardingsphere.infra.executor.kernel.model.ExecutionGroup;
import org.apache.shardingsphere.infra.executor.kernel.model.ExecutionGroupContext;
import org.apache.shardingsphere.infra.executor.kernel.model.ExecutionGroupReportContext;
import org.apache.shardingsphere.infra.executor.sql.context.ExecutionContext;
import org.apache.shardingsphere.infra.executor.sql.context.ExecutionUnit;
import org.apache.shardingsphere.infra.executor.sql.context.SQLUnit;
import org.apache.shardingsphere.infra.executor.sql.execute.engine.ConnectionMode;
import org.apache.shardingsphere.infra.executor.sql.execute.engine.SQLExecutorExceptionHandler;
import org.apache.shardingsphere.infra.executor.sql.execute.engine.driver.jdbc.JDBCExecutionUnit;
import org.apache.shardingsphere.infra.executor.sql.execute.engine.driver.jdbc.JDBCExecutor;
import org.apache.shardingsphere.infra.executor.sql.execute.engine.driver.jdbc.JDBCExecutorCallback;
import org.apache.shardingsphere.infra.executor.sql.execute.result.ExecuteResult;
import org.apache.shardingsphere.infra.executor.sql.execute.result.query.QueryResult;
import org.apache.shardingsphere.infra.executor.sql.execute.result.query.QueryResultMetaData;
import org.apache.shardingsphere.infra.executor.sql.execute.result.query.impl.driver.jdbc.type.memory.JDBCMemoryQueryResult;
import org.apache.shardingsphere.infra.executor.sql.execute.result.query.impl.driver.jdbc.type.stream.JDBCStreamQueryResult;
import org.apache.shardingsphere.infra.executor.sql.execute.result.update.UpdateResult;
import org.apache.shardingsphere.infra.executor.sql.prepare.driver.DriverExecutionPrepareEngine;
import org.apache.shardingsphere.infra.executor.sql.prepare.driver.jdbc.JDBCDriverType;
import org.apache.shardingsphere.infra.executor.sql.prepare.driver.jdbc.StatementOption;
import org.apache.shardingsphere.infra.metadata.database.ShardingSphereDatabase;
import org.apache.shardingsphere.infra.metadata.database.rule.ShardingSphereRuleMetaData;
import org.apache.shardingsphere.infra.parser.SQLParserEngine;
import org.apache.shardingsphere.infra.rule.ShardingSphereRule;
import org.apache.shardingsphere.infra.util.spi.type.typed.TypedSPILoader;
import org.apache.shardingsphere.mode.metadata.MetaDataContexts;
import org.apache.shardingsphere.parser.rule.SQLParserRule;
import org.apache.shardingsphere.proxy.backend.connector.jdbc.statement.JDBCBackendStatement;
import org.apache.shardingsphere.proxy.backend.context.BackendExecutorContext;
import org.apache.shardingsphere.proxy.backend.context.ProxyContext;
import org.apache.shardingsphere.proxy.backend.handler.ProxyBackendHandler;
import org.apache.shardingsphere.proxy.backend.response.header.ResponseHeader;
import org.apache.shardingsphere.proxy.backend.response.header.query.QueryHeader;
import org.apache.shardingsphere.proxy.backend.response.header.query.QueryResponseHeader;
import org.apache.shardingsphere.proxy.backend.response.header.update.UpdateResponseHeader;
import org.apache.shardingsphere.proxy.backend.session.ConnectionSession;
import org.apache.shardingsphere.infra.statistics.monitor.LocalLockTable;
import org.apache.shardingsphere.infra.statistics.monitor.LockMetaData;
import org.apache.shardingsphere.infra.statistics.network.Latency;
import org.apache.shardingsphere.sql.parser.sql.common.statement.SQLStatement;
import org.apache.shardingsphere.sql.parser.sql.common.statement.dml.UpdateStatement;
import org.apache.shardingsphere.sql.parser.sql.common.util.SQLUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.regex.Pattern;

/**
 * Handler for MySQL multi statements.
 */
@Slf4j
public final class MySQLMultiStatementsHandler implements ProxyBackendHandler {
    
    private static final Pattern MULTI_UPDATE_STATEMENTS = Pattern.compile(";(?=\\s*update)", Pattern.CASE_INSENSITIVE);
    
    private static final Pattern MULTI_DELETE_STATEMENTS = Pattern.compile(";(?=\\s*delete)", Pattern.CASE_INSENSITIVE);
    
    private final KernelProcessor kernelProcessor = new KernelProcessor();
    
    private final JDBCExecutor jdbcExecutor;
    
    private final ConnectionSession connectionSession;
    
    private final SQLStatement sqlStatementSample;
    
    private final List<SQLStatement> sqlStatements;
    
    private final MetaDataContexts metaDataContexts = ProxyContext.getInstance().getContextManager().getMetaDataContexts();
    
    private final Map<String, List<ExecutionUnit>> dataSourcesToExecutionUnits = new HashMap<>();
    
    private final Map<String, List<Integer>> dataSourcesToCommandId = new HashMap<>();
    
    private final Map<String, List<QueryContext>> dataSourcesToQueryContext = new HashMap<>();
    
    private ExecutionContext anyExecutionContext;
    
    private boolean isBatchInsert;
    @Setter
    private boolean isLastQuery;
    
    public MySQLMultiStatementsHandler(final ConnectionSession connectionSession, final SQLStatement sqlStatementSample, final String sql, boolean isBatchInsert) {
        jdbcExecutor = new JDBCExecutor(BackendExecutorContext.getInstance().getExecutorEngine(), connectionSession.getConnectionContext());
        connectionSession.getBackendConnection().handleAutoCommit();
        this.connectionSession = connectionSession;
        this.sqlStatementSample = sqlStatementSample;
        Pattern pattern = sqlStatementSample instanceof UpdateStatement ? MULTI_UPDATE_STATEMENTS : MULTI_DELETE_STATEMENTS;
        SQLParserEngine sqlParserEngine = getSQLParserEngine();
        for (String each : extractMultiStatements(pattern, sql)) {
            SQLStatement eachSQLStatement = sqlParserEngine.parse(each, false);
            ExecutionContext executionContext = createExecutionContext(createQueryContext(each, eachSQLStatement));
            if (null == anyExecutionContext) {
                anyExecutionContext = executionContext;
            }
            for (ExecutionUnit eachExecutionUnit : executionContext.getExecutionUnits()) {
                dataSourcesToExecutionUnits.computeIfAbsent(eachExecutionUnit.getDataSourceName(), unused -> new LinkedList<>()).add(eachExecutionUnit);
            }
        }
        this.sqlStatements = null;
        this.isBatchInsert = isBatchInsert;
        isLastQuery = SQLUtils.isLastQuery(sql);
    }
    
    public MySQLMultiStatementsHandler(final ConnectionSession connectionSession, final List<SQLStatement> sqlStatements, final String sql, boolean isBatchInsert) {
        jdbcExecutor = new JDBCExecutor(BackendExecutorContext.getInstance().getExecutorEngine(), connectionSession.getConnectionContext());
        connectionSession.getBackendConnection().handleAutoCommit();
        this.connectionSession = connectionSession;
        this.sqlStatements = sqlStatements;
        this.sqlStatementSample = null;
        this.isBatchInsert = isBatchInsert;
        // Pattern pattern = sqlStatementSample instanceof UpdateStatement ? MULTI_UPDATE_STATEMENTS : MULTI_DELETE_STATEMENTS;
        List<String> sqls = SQLUtils.splitMultiSQL(sql);
        isLastQuery = SQLUtils.isLastQuery(sql);

        assert (sqlStatements.size() == sqls.size());
        
        Map<String, List<ExecutionUnit>> groupExecuteUnits = new HashMap<>();
        for (int i = 0; i < sqlStatements.size(); i++) {
            ExecutionContext executionContext = createExecutionContext(createQueryContext(sqls.get(i), sqlStatements.get(i)));
            String dataSourceName = "";
            if (null == anyExecutionContext) {
                anyExecutionContext = executionContext;
            }
            for (ExecutionUnit eachExecutionUnit : executionContext.getExecutionUnits()) {
                dataSourceName = eachExecutionUnit.getDataSourceName();
                groupExecuteUnits.computeIfAbsent(dataSourceName, unused -> new LinkedList<>()).add(eachExecutionUnit);
                dataSourcesToCommandId.computeIfAbsent(dataSourceName, unused -> new LinkedList<>()).add(i);
            }
            
            dataSourcesToQueryContext.computeIfAbsent(dataSourceName, unused -> new LinkedList<>()).add(executionContext.getQueryContext());
        }

        for (Map.Entry<String, Connection> entry : connectionSession.getBackendConnection().getCachedConnections().entries()) {
            String dataSourceName = entry.getKey().split("\\.")[1];
            ExecutionUnit unit = new ExecutionUnit(dataSourceName, new SQLUnit());
            groupExecuteUnits.computeIfAbsent(dataSourceName, unused -> new LinkedList<>()).add(unit);
        }
        
        for (List<ExecutionUnit> each : groupExecuteUnits.values()) {
            ExecutionUnit first = each.get(0);
            for (int i = 1; i < each.size(); i++) {
                first.CombineExecutionUnit(each.get(i));
            }
            
            // TODO: add prepare stmt
            
            dataSourcesToExecutionUnits.computeIfAbsent(first.getDataSourceName(), unused -> new LinkedList<>()).add(first);
        }
    }
    
    private SQLParserEngine getSQLParserEngine() {
        MetaDataContexts metaDataContexts = ProxyContext.getInstance().getContextManager().getMetaDataContexts();
        SQLParserRule sqlParserRule = metaDataContexts.getMetaData().getGlobalRuleMetaData().getSingleRule(SQLParserRule.class);
        return sqlParserRule.getSQLParserEngine(TypedSPILoader.getService(DatabaseType.class, "MySQL").getType());
    }
    
    private List<String> extractMultiStatements(final Pattern pattern, final String sql) {
        // TODO Multi statements should be split by SQL Parser instead of simple regexp.
        return Arrays.asList(pattern.split(sql));
    }
    
    private QueryContext createQueryContext(final String sql, final SQLStatement sqlStatement) {
        SQLStatementContext<?> sqlStatementContext = SQLStatementContextFactory.newInstance(
                metaDataContexts.getMetaData(), Collections.emptyList(), sqlStatement, connectionSession.getDatabaseName());
        return new QueryContext(sqlStatementContext, sql, Collections.emptyList());
    }
    
    private ExecutionContext createExecutionContext(final QueryContext queryContext) {
        ShardingSphereRuleMetaData globalRuleMetaData = metaDataContexts.getMetaData().getGlobalRuleMetaData();
        ShardingSphereDatabase currentDatabase = metaDataContexts.getMetaData().getDatabase(connectionSession.getDatabaseName());
        SQLAuditEngine.audit(queryContext.getSqlStatementContext(), queryContext.getParameters(), globalRuleMetaData, currentDatabase, null);
        return kernelProcessor.generateExecutionContext(queryContext, currentDatabase, globalRuleMetaData, metaDataContexts.getMetaData().getProps(), connectionSession.getConnectionContext());
    }
    
    @Override
    public List<ResponseHeader> execute() throws SQLException {
        Collection<ShardingSphereRule> rules = metaDataContexts.getMetaData().getDatabase(connectionSession.getDatabaseName()).getRuleMetaData().getRules();
        DriverExecutionPrepareEngine<JDBCExecutionUnit, Connection> prepareEngine = new DriverExecutionPrepareEngine<>(JDBCDriverType.STATEMENT, metaDataContexts.getMetaData().getProps()
                .<Integer>getValue(ConfigurationPropertyKey.MAX_CONNECTIONS_SIZE_PER_QUERY), connectionSession.getBackendConnection(),
                (JDBCBackendStatement) connectionSession.getStatementManager(), new StatementOption(false), rules,
                metaDataContexts.getMetaData().getDatabase(connectionSession.getDatabaseName()).getResourceMetaData().getStorageTypes());
        ExecutionGroupContext<JDBCExecutionUnit> executionGroupContext = prepareEngine.prepare(anyExecutionContext.getRouteContext(), samplingExecutionUnit(),
                new ExecutionGroupReportContext(connectionSession.getDatabaseName(), connectionSession.getGrantee(), connectionSession.getExecutionId()));
        if (isBatchInsert) {
            for (ExecutionGroup<JDBCExecutionUnit> eachGroup : executionGroupContext.getInputGroups()) {
                for (JDBCExecutionUnit each : eachGroup.getInputs()) {
                    prepareBatchedStatement(each);
                }
            }
        }
        
        if (Latency.getInstance().NeedDelay()) {
            boolean needPreAbort = analysisLatency((List<ExecutionGroup<JDBCExecutionUnit>>) executionGroupContext.getInputGroups());
            if (!needPreAbort) {
                throw new SQLException("this transaction is most likely to timeout, pre-abort in harp");
            }
        }

        boolean onePhase = executionGroupContext.getInputGroups().size() == 1;
        for (ExecutionGroup<JDBCExecutionUnit> each: executionGroupContext.getInputGroups()) {
            ExecutionUnit executionUnit = each.getInputs().get(0).getExecutionUnit();
            executionUnit.getSqlUnit().setLastQueryComment(onePhase);
        }
        
        return executeMultiStatements(executionGroupContext);
    }
    
    private boolean analysisLatency(List<ExecutionGroup<JDBCExecutionUnit>> groupUnits) {
        // harp
        if (groupUnits.size() == 0) {
            return true;
        }
        
        int maxLatency = 0;
        
        for (ExecutionGroup<JDBCExecutionUnit> each : groupUnits) {
            ExecutionUnit executionUnit = each.getInputs().get(0).getExecutionUnit();
            String dataSourceName = executionUnit.getDataSourceName();
            
            int srcLat = (int) Latency.getInstance().GetLatency(dataSourceName);
            executionUnit.updateLocalExecuteLatency(srcLat);
            executionUnit.setNetworkLatency(srcLat);
            executionUnit.setHarp(true);
            
            for (QueryContext queryContext : dataSourcesToQueryContext.get(dataSourceName)) {
                String tableName = getTableNameFromSQLStatementContext(queryContext.getSqlStatementContext());
                int key = getKeyFromSQLStatementContext(queryContext.getSqlStatementContext());
                if (Latency.getInstance().NeedPreAbort() || Latency.getInstance().NeedLatencyPredictionAndPreAbort()) {
                    executionUnit.updateProbability(Objects.requireNonNull(LocalLockTable.getInstance().getLockMetaData(tableName, key)).nonBlockProbability());
                }
                if (Latency.getInstance().NeedLatencyPredict() || Latency.getInstance().NeedLatencyPredictionAndPreAbort()){
                    executionUnit.updateLocalExecuteLatency((int) Objects.requireNonNull(LocalLockTable.getInstance().getLockMetaData(tableName, key)).getLatency());
                }
                executionUnit.addKeys(tableName, key);
            }
            
            // System.out.println("probability: " + executionUnit.getAbortProbability() + " latency: " + executionUnit.getLocalExecuteLatency());
            
            // pre-abort
            if (Math.random() < executionUnit.getAbortProbability()) {
                if (groupUnits.size() > 1) {
                    return false;
                }
            }
            
            maxLatency = Math.max(maxLatency, executionUnit.getLocalExecuteLatency());
            // System.out.println("maxLatency: " + maxLatency + "ms");
        }
        
        for (ExecutionGroup<JDBCExecutionUnit> each : groupUnits) {
            ExecutionUnit executionUnit = each.getInputs().get(0).getExecutionUnit();
            String dataSourceName = executionUnit.getDataSourceName();
            
            for (QueryContext queryContext : dataSourcesToQueryContext.get(dataSourceName)) {
                String tableName = getTableNameFromSQLStatementContext(queryContext.getSqlStatementContext());
                int key = getKeyFromSQLStatementContext(queryContext.getSqlStatementContext());
                Objects.requireNonNull(LocalLockTable.getInstance().getLockMetaData(tableName, key)).incProcessing();
            }
        }
        
        for (ExecutionGroup<JDBCExecutionUnit> each : groupUnits) {
            ExecutionUnit executionUnit = each.getInputs().get(0).getExecutionUnit();
            long srcLat = executionUnit.getLocalExecuteLatency();
            executionUnit.SetDelayTime(maxLatency - srcLat);
        }
        
        return true;
    }
    
    private String getTableNameFromSQLStatementContext(SQLStatementContext sqlStatementContext) {
        String tableName = "";
        if (sqlStatementContext instanceof SelectStatementContext) {
            SelectStatementContext selectStatementContext = (SelectStatementContext) sqlStatementContext;
            tableName = selectStatementContext.getTableName().get(0);
        } else if (sqlStatementContext instanceof UpdateStatementContext) {
            UpdateStatementContext updateStatementContext = (UpdateStatementContext) sqlStatementContext;
            tableName = updateStatementContext.getTableName().get(0);
        }
        return tableName;
    }
    
    private int getKeyFromSQLStatementContext(SQLStatementContext sqlStatementContext) {
        int key = -1;
        if (sqlStatementContext instanceof SelectStatementContext) {
            SelectStatementContext selectStatementContext = (SelectStatementContext) sqlStatementContext;
            key = selectStatementContext.getKey().get(0);
        } else if (sqlStatementContext instanceof UpdateStatementContext) {
            UpdateStatementContext updateStatementContext = (UpdateStatementContext) sqlStatementContext;
            key = updateStatementContext.getKey().get(0);
        }
        return key;
    }
    
    private Collection<ExecutionUnit> samplingExecutionUnit() {
        Collection<ExecutionUnit> result = new LinkedList<>();
        for (List<ExecutionUnit> each : dataSourcesToExecutionUnits.values()) {
            result.add(each.get(0));
        }
        return result;
    }
    
    private void prepareBatchedStatement(final JDBCExecutionUnit each) throws SQLException {
        Statement statement = each.getStorageResource();
        for (ExecutionUnit eachExecutionUnit : dataSourcesToExecutionUnits.get(each.getExecutionUnit().getDataSourceName())) {
            statement.addBatch(eachExecutionUnit.getSqlUnit().getSql());
        }
    }
    
    private QueryHeader generateQueryHeader(QueryResultMetaData meta, int colIndex) throws SQLException {
        String schemaName = connectionSession.getDatabaseName();
        
        return new QueryHeader(schemaName,
                meta.getTableName(colIndex),
                meta.getColumnLabel(colIndex),
                meta.getColumnName(colIndex),
                meta.getColumnType(colIndex),
                meta.getColumnTypeName(colIndex),
                meta.getColumnLength(colIndex),
                meta.getDecimals(colIndex),
                meta.isSigned(colIndex),
                colIndex == 0,
                meta.isNotNull(colIndex),
                meta.isAutoIncrement(colIndex));
    }
    
    private List<ResponseHeader> executeMultiStatements(final ExecutionGroupContext<JDBCExecutionUnit> executionGroupContext) throws SQLException {
        boolean isExceptionThrown = SQLExecutorExceptionHandler.isExceptionThrown();
        List<ResponseHeader> result = new LinkedList<>();
        Map<String, DatabaseType> storageTypes = metaDataContexts.getMetaData().getDatabase(connectionSession.getDatabaseName()).getResourceMetaData().getStorageTypes();
        if (isBatchInsert) {
            JDBCExecutorCallback<int[]> callback = new BatchedInsertJDBCExecutorCallback(storageTypes, sqlStatementSample, isExceptionThrown);
            List<int[]> executeResults = jdbcExecutor.execute(executionGroupContext, callback);
            int updated = 0;
            for (int[] eachResult : executeResults) {
                for (int each : eachResult) {
                    updated += each;
                }
            }
            result.add(new UpdateResponseHeader(sqlStatementSample, Collections.singletonList(new UpdateResult(updated, 0L))));
        } else {
            JDBCExecutorCallback<List<ExecuteResult>> callback = new BatchedJDBCExecutorCallback(storageTypes, sqlStatementSample, isExceptionThrown);
            try {
                long start = System.nanoTime() / 1000000;
                List<List<ExecuteResult>> executeResults = jdbcExecutor.execute(executionGroupContext, callback);
//                System.out.println("exectution time: " + (System.nanoTime() / 1000000 - start) + "ms; " + System.nanoTime() / 1000000 + "ms");

                feedback((List<ExecutionGroup<JDBCExecutionUnit>>) executionGroupContext.getInputGroups(), true);
                boolean first = false;
                for (List<ExecuteResult> each : executeResults) {
                    for (ExecuteResult obj : each) {
                        if (obj instanceof QueryResult) {
                            QueryResultMetaData meta = ((QueryResult) obj).getMetaData();
                            int columnCount = meta.getColumnCount();
                            List<QueryHeader> headers = new ArrayList<>(columnCount);
                            
                            for (int i = 1; i <= columnCount; i++) {
                                headers.add(generateQueryHeader(meta, i));
                            }
                            
                            if (!first) {
                                result.add(new QueryResponseHeader(headers));
                                first = true;
                            }
                            try {
                                ((QueryResult) obj).close();
                            } catch (SQLException ignore) {
                            }
                        } else {
                            if (!first) {
                                result.add(new UpdateResponseHeader(sqlStatementSample,
                                        Collections.singletonList(new UpdateResult(((UpdateResult) obj).getUpdateCount(), ((UpdateResult) obj).getLastInsertId()))));
                                first = true;
                            }
                        }
                    }
                    each.clear();
                }
            } catch (Exception ex) {
                feedback((List<ExecutionGroup<JDBCExecutionUnit>>) executionGroupContext.getInputGroups(), false);
                throw ex;
            }
        }
        
        return result;
    }
    
    private void feedback(List<ExecutionGroup<JDBCExecutionUnit>> groupUnits, boolean isFinish) {
        double networkThreshold = Latency.getInstance().getLongestLatency();
        
        for (ExecutionGroup<JDBCExecutionUnit> each : groupUnits) {
            ExecutionUnit executionUnit = each.getInputs().get(0).getExecutionUnit();
            String dataSourceName = executionUnit.getDataSourceName();
            
            double localExecuteTime = Math.max(0, executionUnit.getRealExecuteLatency() - Latency.getInstance().GetLatency(dataSourceName));

            if (isFinish && localExecuteTime < 1e-5) {
                for (Map.Entry<String, List<Integer>> tableToKeys : executionUnit.getKeys().entrySet()) {
                    for (Integer key : tableToKeys.getValue()) {
                        LockMetaData lockMetaData = LocalLockTable.getInstance().getLockMetaData(tableToKeys.getKey(), key);
                        Objects.requireNonNull(lockMetaData).decProcessing();
                        lockMetaData.incCount();
                        lockMetaData.incSuccessCount();
                    }
                }
                continue;
            }
            
            for (Map.Entry<String, List<Integer>> tableToKeys : executionUnit.getKeys().entrySet()) {
                double totalWeight = 0;
                if (isFinish) {
                    for (Integer key : tableToKeys.getValue()) {
                        totalWeight += Objects.requireNonNull(LocalLockTable.getInstance().getLockMetaData(tableToKeys.getKey(), key)).getLatency();
                    }
                }
                
                for (Integer key : tableToKeys.getValue()) {
                    LockMetaData lockMetaData = LocalLockTable.getInstance().getLockMetaData(tableToKeys.getKey(), key);
                    Objects.requireNonNull(lockMetaData).decProcessing();
                    lockMetaData.incCount();
                    if (isFinish) {
                        double singleLatency = localExecuteTime * lockMetaData.getLatency() / Math.max(totalWeight, 0.0001);

                        if (singleLatency < networkThreshold) {
                            lockMetaData.incSuccessCount();
                            Objects.requireNonNull(LocalLockTable.getInstance().getLockMetaData(tableToKeys.getKey(), key)).updateLatency(singleLatency);
                        }
                    }
                }
            }
        }
    }
    
    private static class BatchedJDBCExecutorCallback extends JDBCExecutorCallback<List<ExecuteResult>> {
        
        BatchedJDBCExecutorCallback(final Map<String, DatabaseType> storageTypes, final SQLStatement sqlStatement, final boolean isExceptionThrown) {
            super(TypedSPILoader.getService(DatabaseType.class, "MySQL"), storageTypes, sqlStatement, isExceptionThrown);
        }
        
        @Override
        protected List<ExecuteResult> executeSQL(final String sql, final Statement statement, final ConnectionMode connectionMode, final DatabaseType storageType) throws SQLException {
            boolean resultsAvailable = false;
            try {
                long start = System.nanoTime();
                resultsAvailable = statement.execute(sql);
                System.out.println("True execute time: " + ((System.nanoTime() - start) / 1000) + "us");
                
                List<ExecuteResult> list = new ArrayList<>();
                while (true) {
                    if (resultsAvailable) {
                        ResultSet rs = statement.getResultSet();
                        list.add(createQueryResult(rs, connectionMode, storageType));
                    } else {
                        int update_cnt = statement.getUpdateCount();
                        if (update_cnt != -1) {
                            list.add(new UpdateResult(update_cnt, 0));
                        } else {
                            break;
                        }
                    }
                    
                    resultsAvailable = statement.getMoreResults();
                }
                
                return list;
            } catch (SQLException e) {
                // System.out.println("sql: " + sql);
                overlookResult(resultsAvailable, statement);
                e.printStackTrace();
                throw e;
            } finally {
                statement.close();
            }
        }
        
        @SuppressWarnings("OptionalContainsCollection")
        @Override
        protected Optional<List<ExecuteResult>> getSaneResult(final SQLStatement sqlStatement, final SQLException ex) {
            return Optional.empty();
        }
        
        private QueryResult createQueryResult(final ResultSet resultSet, final ConnectionMode connectionMode, final DatabaseType storageType) throws SQLException {
            return ConnectionMode.MEMORY_STRICTLY == connectionMode ? new JDBCStreamQueryResult(resultSet) : new JDBCMemoryQueryResult(resultSet, storageType);
        }
        
        private void overlookResult(boolean resultsAvailable, final Statement statement) throws SQLException {
            try {
                while (true) {
                    if (resultsAvailable) {
                        ResultSet rs = statement.getResultSet();
                    } else {
                        int update_cnt = statement.getUpdateCount();
                        // TODO:
                        if (update_cnt != -1) {
                        } else {
                            break;
                        }
                    }
                    
                    resultsAvailable = statement.getMoreResults();
                }
            } catch (final SQLException ex) {
                ex.printStackTrace();
                throw ex;
            }
        }
    }
    
    private static class BatchedInsertJDBCExecutorCallback extends JDBCExecutorCallback<int[]> {
        
        BatchedInsertJDBCExecutorCallback(final Map<String, DatabaseType> storageTypes, final SQLStatement sqlStatement, final boolean isExceptionThrown) {
            super(TypedSPILoader.getService(DatabaseType.class, "MySQL"), storageTypes, sqlStatement, isExceptionThrown);
        }
        
        protected int[] executeSQL(final String sql, final Statement statement, final ConnectionMode connectionMode, final DatabaseType storageType) throws SQLException {
            try {
                return statement.executeBatch();
            } finally {
                statement.close();
            }
        }
        
        @SuppressWarnings("OptionalContainsCollection")
        @Override
        protected Optional<int[]> getSaneResult(final SQLStatement sqlStatement, final SQLException ex) {
            return Optional.empty();
        }
    }
}
