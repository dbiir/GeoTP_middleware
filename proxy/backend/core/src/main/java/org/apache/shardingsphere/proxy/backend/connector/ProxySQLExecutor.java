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

package org.apache.shardingsphere.proxy.backend.connector;

import org.apache.shardingsphere.dialect.exception.transaction.TableModifyInTransactionException;
import org.apache.shardingsphere.infra.binder.statement.SQLStatementContext;
import org.apache.shardingsphere.infra.binder.statement.dml.SelectStatementContext;
import org.apache.shardingsphere.infra.binder.statement.dml.UpdateStatementContext;
import org.apache.shardingsphere.infra.binder.type.TableAvailable;
import org.apache.shardingsphere.infra.config.props.ConfigurationPropertyKey;
import org.apache.shardingsphere.infra.context.ConnectionContext;
import org.apache.shardingsphere.infra.context.transaction.TransactionConnectionContext;
import org.apache.shardingsphere.infra.database.type.DatabaseType;
import org.apache.shardingsphere.infra.executor.kernel.ExecutorEngine;
import org.apache.shardingsphere.infra.executor.kernel.model.ExecutionGroup;
import org.apache.shardingsphere.infra.executor.kernel.model.ExecutionGroupContext;
import org.apache.shardingsphere.infra.executor.kernel.model.ExecutionGroupReportContext;
import org.apache.shardingsphere.infra.executor.sql.context.ExecutionContext;
import org.apache.shardingsphere.infra.executor.sql.execute.engine.SQLExecutorExceptionHandler;
import org.apache.shardingsphere.infra.executor.sql.execute.engine.driver.jdbc.JDBCExecutionUnit;
import org.apache.shardingsphere.infra.executor.sql.execute.engine.driver.jdbc.JDBCExecutor;
import org.apache.shardingsphere.infra.executor.sql.execute.engine.raw.RawExecutor;
import org.apache.shardingsphere.infra.executor.sql.execute.engine.raw.RawSQLExecutionUnit;
import org.apache.shardingsphere.infra.executor.sql.execute.engine.raw.callback.RawSQLExecutorCallback;
import org.apache.shardingsphere.infra.executor.sql.execute.result.ExecuteResult;
import org.apache.shardingsphere.infra.executor.sql.prepare.driver.DriverExecutionPrepareEngine;
import org.apache.shardingsphere.infra.executor.sql.prepare.driver.jdbc.StatementOption;
import org.apache.shardingsphere.infra.executor.sql.prepare.raw.RawExecutionPrepareEngine;
import org.apache.shardingsphere.infra.rule.ShardingSphereRule;
import org.apache.shardingsphere.infra.rule.identifier.type.RawExecutionRule;
import org.apache.shardingsphere.infra.util.exception.ShardingSpherePreconditions;
import org.apache.shardingsphere.infra.util.spi.ShardingSphereServiceLoader;
import org.apache.shardingsphere.infra.util.spi.type.typed.TypedSPILoader;
import org.apache.shardingsphere.proxy.backend.connector.jdbc.executor.ProxyJDBCExecutor;
import org.apache.shardingsphere.proxy.backend.connector.jdbc.statement.JDBCBackendStatement;
import org.apache.shardingsphere.proxy.backend.connector.sane.SaneQueryResultEngine;
import org.apache.shardingsphere.proxy.backend.context.BackendExecutorContext;
import org.apache.shardingsphere.proxy.backend.context.ProxyContext;
import org.apache.shardingsphere.proxy.backend.session.ConnectionSession;
import org.apache.shardingsphere.proxy.backend.session.transaction.TransactionStatus;
import org.apache.shardingsphere.infra.statistics.monitor.LocalLockTable;
import org.apache.shardingsphere.infra.statistics.monitor.LockMetaData;
import org.apache.shardingsphere.infra.statistics.network.Latency;
import org.apache.shardingsphere.sql.parser.sql.common.segment.dml.expr.BinaryOperationExpression;
import org.apache.shardingsphere.sql.parser.sql.common.segment.dml.expr.simple.LiteralExpressionSegment;
import org.apache.shardingsphere.sql.parser.sql.common.segment.generic.table.SimpleTableSegment;
import org.apache.shardingsphere.sql.parser.sql.common.statement.SQLStatement;
import org.apache.shardingsphere.sql.parser.sql.common.statement.ddl.*;
import org.apache.shardingsphere.sql.parser.sql.common.statement.dml.SelectStatement;
import org.apache.shardingsphere.sql.parser.sql.common.statement.dml.UpdateStatement;
import org.apache.shardingsphere.sql.parser.sql.dialect.statement.mysql.dml.MySQLInsertStatement;
import org.apache.shardingsphere.sql.parser.sql.dialect.statement.mysql.dml.MySQLUpdateStatement;
import org.apache.shardingsphere.sql.parser.sql.dialect.statement.opengauss.OpenGaussStatement;
import org.apache.shardingsphere.sql.parser.sql.dialect.statement.opengauss.ddl.OpenGaussCursorStatement;
import org.apache.shardingsphere.sql.parser.sql.dialect.statement.postgresql.PostgreSQLStatement;
import org.apache.shardingsphere.transaction.api.TransactionType;
import org.apache.shardingsphere.transaction.spi.TransactionHook;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Proxy SQL Executor.
 */
public final class ProxySQLExecutor {
    
    private final String type;
    
    private final BackendConnection backendConnection;
    
    private final ProxyJDBCExecutor jdbcExecutor;
    
    private final RawExecutor rawExecutor;
    
    private final Collection<TransactionHook> transactionHooks = ShardingSphereServiceLoader.getServiceInstances(TransactionHook.class);
    
    public ProxySQLExecutor(final String type, final BackendConnection backendConnection, final DatabaseConnector databaseConnector) {
        this.type = type;
        this.backendConnection = backendConnection;
        ExecutorEngine executorEngine = BackendExecutorContext.getInstance().getExecutorEngine();
        ConnectionContext connectionContext = backendConnection.getConnectionSession().getConnectionContext();
        jdbcExecutor = new ProxyJDBCExecutor(type, backendConnection.getConnectionSession(), databaseConnector, new JDBCExecutor(executorEngine, connectionContext));
        rawExecutor = new RawExecutor(executorEngine, connectionContext);
    }
    
    /**
     * Check execute prerequisites.
     *
     * @param executionContext execution context
     */
    public void checkExecutePrerequisites(final ExecutionContext executionContext) {
        ShardingSpherePreconditions.checkState(isValidExecutePrerequisites(executionContext), () -> new TableModifyInTransactionException(getTableName(executionContext)));
    }
    
    private boolean isValidExecutePrerequisites(final ExecutionContext executionContext) {
        return !isExecuteDDLInXATransaction(executionContext.getSqlStatementContext().getSqlStatement())
                && !isExecuteDDLInPostgreSQLOpenGaussTransaction(executionContext.getSqlStatementContext().getSqlStatement());
    }
    
    private boolean isExecuteDDLInXATransaction(final SQLStatement sqlStatement) {
        TransactionStatus transactionStatus = backendConnection.getConnectionSession().getTransactionStatus();
        return TransactionType.XA == transactionStatus.getTransactionType() && transactionStatus.isInTransaction() && isUnsupportedDDLStatement(sqlStatement);
    }
    
    private boolean isExecuteDDLInPostgreSQLOpenGaussTransaction(final SQLStatement sqlStatement) {
        // TODO implement DDL statement commit/rollback in PostgreSQL/openGauss transaction
        boolean isPostgreSQLOpenGaussStatement = isPostgreSQLOrOpenGaussStatement(sqlStatement);
        boolean isSupportedStatement = isSupportedSQLStatement(sqlStatement);
        return sqlStatement instanceof DDLStatement && !isSupportedStatement && isPostgreSQLOpenGaussStatement && backendConnection.getConnectionSession().getTransactionStatus().isInTransaction();
    }
    
    private boolean isSupportedSQLStatement(final SQLStatement sqlStatement) {
        return isCursorStatement(sqlStatement) || sqlStatement instanceof TruncateStatement;
    }
    
    private boolean isCursorStatement(final SQLStatement sqlStatement) {
        return sqlStatement instanceof OpenGaussCursorStatement
                || sqlStatement instanceof CloseStatement || sqlStatement instanceof MoveStatement || sqlStatement instanceof FetchStatement;
    }
    
    private boolean isUnsupportedDDLStatement(final SQLStatement sqlStatement) {
        if (isPostgreSQLOrOpenGaussStatement(sqlStatement) && isSupportedSQLStatement(sqlStatement)) {
            return false;
        }
        return sqlStatement instanceof DDLStatement;
    }
    
    private boolean isPostgreSQLOrOpenGaussStatement(final SQLStatement sqlStatement) {
        return sqlStatement instanceof PostgreSQLStatement || sqlStatement instanceof OpenGaussStatement;
    }
    
    private static String getTableName(final ExecutionContext executionContext) {
        return executionContext.getSqlStatementContext() instanceof TableAvailable && !((TableAvailable) executionContext.getSqlStatementContext()).getAllTables().isEmpty()
                ? ((TableAvailable) executionContext.getSqlStatementContext()).getAllTables().iterator().next().getTableName().getIdentifier().getValue()
                : "unknown_table";
    }
    
    /**
     * Execute SQL.
     *
     * @param executionContext execution context
     * @return execute results
     * @throws SQLException SQL exception
     */
    public List<ExecuteResult> execute(final ExecutionContext executionContext) throws SQLException {
        String databaseName = backendConnection.getConnectionSession().getDatabaseName();
        Collection<ShardingSphereRule> rules = ProxyContext.getInstance().getContextManager().getMetaDataContexts().getMetaData().getDatabase(databaseName).getRuleMetaData().getRules();
        int maxConnectionsSizePerQuery = ProxyContext.getInstance()
                .getContextManager().getMetaDataContexts().getMetaData().getProps().<Integer>getValue(ConfigurationPropertyKey.MAX_CONNECTIONS_SIZE_PER_QUERY);
        boolean isReturnGeneratedKeys = executionContext.getSqlStatementContext().getSqlStatement() instanceof MySQLInsertStatement;
        return hasRawExecutionRule(rules) ? rawExecute(executionContext, rules, maxConnectionsSizePerQuery)
                : useDriverToExecute(executionContext, rules, maxConnectionsSizePerQuery, isReturnGeneratedKeys, SQLExecutorExceptionHandler.isExceptionThrown());
    }
    
    private boolean hasRawExecutionRule(final Collection<ShardingSphereRule> rules) {
        for (ShardingSphereRule each : rules) {
            if (each instanceof RawExecutionRule) {
                return true;
            }
        }
        return false;
    }
    
    private List<ExecuteResult> rawExecute(final ExecutionContext executionContext, final Collection<ShardingSphereRule> rules, final int maxConnectionsSizePerQuery) throws SQLException {
        RawExecutionPrepareEngine prepareEngine = new RawExecutionPrepareEngine(maxConnectionsSizePerQuery, rules);
        ExecutionGroupContext<RawSQLExecutionUnit> executionGroupContext;
        try {
            executionGroupContext = prepareEngine.prepare(executionContext.getRouteContext(), executionContext.getExecutionUnits(), new ExecutionGroupReportContext(
                    backendConnection.getConnectionSession().getDatabaseName(), backendConnection.getConnectionSession().getGrantee(), backendConnection.getConnectionSession().getExecutionId()));
        } catch (final SQLException ex) {
            return getSaneExecuteResults(executionContext, ex);
        }
        // TODO handle query header
        return rawExecutor.execute(executionGroupContext, executionContext.getQueryContext(), new RawSQLExecutorCallback());
    }
    
    private List<ExecuteResult> useDriverToExecute(final ExecutionContext executionContext, final Collection<ShardingSphereRule> rules,
                                                   final int maxConnectionsSizePerQuery, final boolean isReturnGeneratedKeys, final boolean isExceptionThrown) throws SQLException {
        JDBCBackendStatement statementManager = (JDBCBackendStatement) backendConnection.getConnectionSession().getStatementManager();
        DriverExecutionPrepareEngine<JDBCExecutionUnit, Connection> prepareEngine = new DriverExecutionPrepareEngine<>(
                type, maxConnectionsSizePerQuery, backendConnection, statementManager, new StatementOption(isReturnGeneratedKeys), rules,
                ProxyContext.getInstance().getDatabase(backendConnection.getConnectionSession().getDatabaseName()).getResourceMetaData().getStorageTypes());
        ExecutionGroupContext<JDBCExecutionUnit> executionGroupContext;
        try {
            executionGroupContext = prepareEngine.prepare(executionContext.getRouteContext(), executionContext.getExecutionUnits(), new ExecutionGroupReportContext(
                    backendConnection.getConnectionSession().getDatabaseName(), backendConnection.getConnectionSession().getGrantee(), backendConnection.getConnectionSession().getExecutionId()));
        } catch (final SQLException ex) {
            return getSaneExecuteResults(executionContext, ex);
        }
        
        boolean needStat = LocalLockTable.getInstance().needStatistic();
        boolean op = false;
        boolean isHot = false;
        String tableName = "";
        int idx = -1;
        long startTime = 0;
        double executionTime;
        SQLStatement sqlStatement = executionContext.getQueryContext().getSqlStatementContext().getSqlStatement();
        if (sqlStatement instanceof SelectStatement) {
            if (((SelectStatement) sqlStatement).getFrom() != null && ((SelectStatement) sqlStatement).getFrom() instanceof SimpleTableSegment) {
                tableName = ((SimpleTableSegment) ((SelectStatement) sqlStatement).getFrom()).getTableName().getIdentifier().getValue();
                if (LocalLockTable.getInstance().isRegisterTable(tableName)) {
                    op = true;
                    isHot = true;
                }
            }
            if (isHot && ((SelectStatement) sqlStatement).getWhere().isPresent() &&
                    ((SelectStatement) sqlStatement).getWhere().get().getExpr() instanceof BinaryOperationExpression) {
                idx = (int) ((LiteralExpressionSegment) ((BinaryOperationExpression) ((SelectStatement) sqlStatement).getWhere().get().getExpr()).getRight()).getLiterals();
            }
        } else if (sqlStatement instanceof UpdateStatement) {
            if (((MySQLUpdateStatement) sqlStatement).getTable() != null && ((MySQLUpdateStatement) sqlStatement).getTable() instanceof SimpleTableSegment) {
                tableName = ((SimpleTableSegment) ((MySQLUpdateStatement) sqlStatement).getTable()).getTableName().getIdentifier().getValue();
                if (LocalLockTable.getInstance().isRegisterTable(tableName)) {
                    op = false;
                }
            }
            // if (((UpdateStatement) sqlStatement).getWhere().isPresent() &&
            // (((UpdateStatement) sqlStatement).getWhere().get().getExpr()) instanceof BinaryOperationExpression) {
            // idx = (int) ((LiteralExpressionSegment) ((BinaryOperationExpression) ((UpdateStatement) sqlStatement).getWhere().get().getExpr()).getRight()).getLiterals();
            // }
        }
        LockMetaData lockMetaData = LocalLockTable.getInstance().getLockMetaData(tableName, idx);
        
        // boolean needPreAbort = analyseSingleSQL(tableName, idx);
        // if (!needPreAbort) {
        // throw new SQLException("this transaction is most likely to timeout, pre-abort in harp");
        // }
        
        executeTransactionHooksBeforeExecuteSQL(backendConnection.getConnectionSession());
        if (needStat && idx >= 0)
            startTime = System.nanoTime();
        List<ExecuteResult> results;
        try {
            if (Latency.getInstance().NeedDelay()) {
                startTime = System.nanoTime();
                if (lockMetaData != null) {
                    lockMetaData.incProcessing();
                }
            }
            
            results = jdbcExecutor.execute(executionContext.getQueryContext(), executionGroupContext, isReturnGeneratedKeys, isExceptionThrown);
            if (Latency.getInstance().NeedDelay()) {
                if (lockMetaData != null) {
                    lockMetaData.incCount();
                    lockMetaData.decProcessing();
                    double networkThreshold = Latency.getInstance().getLongestLatency();
                    executionTime = (System.nanoTime() - startTime) * 1.0 / 1000000;
                    if (executionTime < 2 * networkThreshold) { // 2RTT
                        lockMetaData.incSuccessCount();
                        lockMetaData.updateLatency(executionTime);
                    }
                }
            }
        } catch (Exception ex) {
            if (lockMetaData != null) {
                lockMetaData.incCount();
                lockMetaData.decProcessing();
            }
            throw ex;
        }
        if (needStat && idx >= 0) {
            LocalLockTable.getInstance().updateLockTime(tableName, idx, (System.nanoTime() - startTime) * 1.0 / 1000000, op);
        }
        return results;
    }
    
    private boolean analyseSingleSQL(String tableName, int key) {
        LockMetaData lockMetaData = LocalLockTable.getInstance().getLockMetaData(tableName, key);
        if (lockMetaData == null)
            return true;
        double p = 1 - lockMetaData.nonBlockProbability();
        return !(Math.random() < p);
    }
    
    private void executeTransactionHooksBeforeExecuteSQL(final ConnectionSession connectionSession) throws SQLException {
        if (!getTransactionContext(connectionSession).isInTransaction()) {
            return;
        }
        for (TransactionHook each : transactionHooks) {
            each.beforeExecuteSQL(connectionSession.getBackendConnection().getCachedConnections().values(), getTransactionContext(connectionSession), connectionSession.getIsolationLevel());
        }
    }
    
    private TransactionConnectionContext getTransactionContext(final ConnectionSession connectionSession) {
        return connectionSession.getBackendConnection().getConnectionSession().getConnectionContext().getTransactionContext();
    }
    
    private List<ExecuteResult> getSaneExecuteResults(final ExecutionContext executionContext, final SQLException originalException) throws SQLException {
        DatabaseType databaseType = ProxyContext.getInstance().getDatabase(backendConnection.getConnectionSession().getDatabaseName()).getProtocolType();
        Optional<ExecuteResult> executeResult = TypedSPILoader.getService(SaneQueryResultEngine.class, databaseType.getType())
                .getSaneQueryResult(executionContext.getSqlStatementContext().getSqlStatement(), originalException);
        if (executeResult.isPresent()) {
            return Collections.singletonList(executeResult.get());
        }
        throw originalException;
    }
}
