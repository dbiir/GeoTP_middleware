package org.apache.shardingsphere.proxy.frontend.postgresql.command.query.simple;

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
import org.apache.shardingsphere.proxy.backend.txnsails.LockType;
import org.apache.shardingsphere.proxy.backend.txnsails.PreValidationInfo;
import org.apache.shardingsphere.sql.parser.sql.common.statement.SQLStatement;
import org.apache.shardingsphere.sql.parser.sql.common.statement.dal.EmptyStatement;
import org.apache.shardingsphere.sql.parser.sql.common.util.SQLUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.regex.Pattern;

@Slf4j
public class PostgreSQLMultiStatementsHandler implements ProxyBackendHandler {
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

  public PostgreSQLMultiStatementsHandler(final ConnectionSession connectionSession, final List<SQLStatement> sqlStatements, final String sql) {
    jdbcExecutor = new JDBCExecutor(BackendExecutorContext.getInstance().getExecutorEngine(), connectionSession.getConnectionContext());
    connectionSession.getBackendConnection().handleAutoCommit();
    this.connectionSession = connectionSession;
    this.sqlStatements = sqlStatements;
    this.sqlStatementSample = null;
    // Pattern pattern = sqlStatementSample instanceof UpdateStatement ? MULTI_UPDATE_STATEMENTS : MULTI_DELETE_STATEMENTS;
    List<String> sqls = SQLUtils.splitMultiSQL(sql);

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

    for (List<ExecutionUnit> each : groupExecuteUnits.values()) {
      ExecutionUnit first = each.get(0);
      for (int i = 1; i < each.size(); i++) {
        first.CombineExecutionUnit(each.get(i));
      }

      dataSourcesToExecutionUnits.computeIfAbsent(first.getDataSourceName(), unused -> new LinkedList<>()).add(first);
    }
  }

  private ExecutionContext createExecutionContext(final QueryContext queryContext) {
    ShardingSphereRuleMetaData globalRuleMetaData = metaDataContexts.getMetaData().getGlobalRuleMetaData();
    ShardingSphereDatabase currentDatabase = metaDataContexts.getMetaData().getDatabase(connectionSession.getDatabaseName());
    SQLAuditEngine.audit(queryContext.getSqlStatementContext(), queryContext.getParameters(), globalRuleMetaData, currentDatabase, null);
    return kernelProcessor.generateExecutionContext(queryContext, currentDatabase, globalRuleMetaData, metaDataContexts.getMetaData().getProps(), connectionSession.getConnectionContext());
  }

  private QueryContext createQueryContext(final String sql, final SQLStatement sqlStatement) {
    SQLStatementContext<?> sqlStatementContext = SQLStatementContextFactory.newInstance(
            metaDataContexts.getMetaData(), Collections.emptyList(), sqlStatement, connectionSession.getDatabaseName());
    return new QueryContext(sqlStatementContext, sql, Collections.emptyList());
  }

  private Collection<ExecutionUnit> samplingExecutionUnit() {
    Collection<ExecutionUnit> result = new LinkedList<>();
    for (List<ExecutionUnit> each : dataSourcesToExecutionUnits.values()) {
      result.add(each.get(0));
    }
    return result;
  }

  @Override
  public List<ResponseHeader> execute() throws SQLException {
    Collection<ShardingSphereRule> rules = metaDataContexts.getMetaData().getDatabase(connectionSession.getDatabaseName()).getRuleMetaData().getRules();
    DriverExecutionPrepareEngine<JDBCExecutionUnit, Connection> prepareEngine = new DriverExecutionPrepareEngine<>(
            JDBCDriverType.STATEMENT,
            metaDataContexts.getMetaData().getProps().<Integer>getValue(ConfigurationPropertyKey.MAX_CONNECTIONS_SIZE_PER_QUERY),
            connectionSession.getBackendConnection(),
            (JDBCBackendStatement) connectionSession.getStatementManager(),
            new StatementOption(false),
            rules,
            metaDataContexts.getMetaData().getDatabase(connectionSession.getDatabaseName()).getResourceMetaData().getStorageTypes()
    );
    ExecutionGroupContext<JDBCExecutionUnit> executionGroupContext = prepareEngine.prepare(anyExecutionContext.getRouteContext(), samplingExecutionUnit(),
            new ExecutionGroupReportContext(connectionSession.getDatabaseName(), connectionSession.getGrantee(), connectionSession.getExecutionId()));

    boolean onePhase = executionGroupContext.getInputGroups().size() == 1;
    prepareValidationSet((List<ExecutionGroup<JDBCExecutionUnit>>) executionGroupContext.getInputGroups());

    return executeMultiStatements(executionGroupContext);
  }

  private void prepareValidationSet(List<ExecutionGroup<JDBCExecutionUnit>> groupUnits) {
    if (groupUnits.isEmpty()) {
      return;
    }

    for (ExecutionGroup<JDBCExecutionUnit> each : groupUnits) {
      ExecutionUnit executionUnit = each.getInputs().get(0).getExecutionUnit();
      String dataSourceName = executionUnit.getDataSourceName();
      for (QueryContext queryContext : dataSourcesToQueryContext.get(dataSourceName)) {
        String tableName = getTableNameFromSQLStatementContext(queryContext.getSqlStatementContext());
        if (!tableName.contains("usertable")) {
          continue;
        }
        int key = getKeyFromSQLStatementContext(queryContext.getSqlStatementContext());
        // find the validation lock
        if (key != -1) {
          if (queryContext.getSqlStatementContext() instanceof SelectStatementContext)
            this.connectionSession.addValidationInfos(new PreValidationInfo(tableName, key, LockType.SH));
          else if (queryContext.getSqlStatementContext() instanceof UpdateStatementContext)
            this.connectionSession.addValidationInfos(new PreValidationInfo(tableName, key, LockType.EX));
        }

        executionUnit.addKeys(tableName, key);
      }
    }
  }

  private static class BatchedJDBCExecutorCallback extends JDBCExecutorCallback<List<ExecuteResult>> {

    BatchedJDBCExecutorCallback(final Map<String, DatabaseType> storageTypes, final SQLStatement sqlStatement, final boolean isExceptionThrown) {
      super(TypedSPILoader.getService(DatabaseType.class, "PostgreSQL"), storageTypes, sqlStatement, isExceptionThrown);
    }

    @Override
    protected List<ExecuteResult> executeSQL(final String sql, final Statement statement, final ConnectionMode connectionMode, final DatabaseType storageType) throws SQLException {
      boolean resultsAvailable = false;
      try {
        resultsAvailable = statement.execute(sql);

        List<ExecuteResult> list = new ArrayList<>();
        while (true) {
          if (resultsAvailable) {
            ResultSet rs = statement.getResultSet();
            QueryResult res = createQueryResult(rs, connectionMode, storageType);
            if (res instanceof JDBCMemoryQueryResult) {
              ((JDBCMemoryQueryResult) res).version = rs.getLong(1);
              ((JDBCMemoryQueryResult) res).sql = sql;
            }
            list.add(res);
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
            if (update_cnt != -1) {
            } else {
              break;
            }
          }

          resultsAvailable = statement.getMoreResults();
        }
      } catch (final SQLException ex) {
        log.error("An error occurred while processing the statement.", ex);
        throw ex;
      }
    }
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

  private List<SQLStatement> parseSql(final String sql, final DatabaseType databaseType) {
    List<SQLStatement> result = new LinkedList<>();
    if (SQLUtils.trimComment(sql).isEmpty()) {
      result.add(new EmptyStatement());
      return result;
    }
    List<String> singleSqls = SQLUtils.splitMultiSQL(sql);
    if (singleSqls.isEmpty()) {
      result.add(new EmptyStatement());
    } else {
      MetaDataContexts metaDataContexts = ProxyContext.getInstance().getContextManager().getMetaDataContexts();
      SQLParserRule sqlParserRule = metaDataContexts.getMetaData().getGlobalRuleMetaData().getSingleRule(SQLParserRule.class);
      for (String each : singleSqls) {
        result.add(sqlParserRule.getSQLParserEngine(databaseType.getType()).parse(each, false));
      }
    }
    return result;
  }

  private List<ResponseHeader> executeMultiStatements(final ExecutionGroupContext<JDBCExecutionUnit> executionGroupContext) throws SQLException {
    boolean isExceptionThrown = SQLExecutorExceptionHandler.isExceptionThrown();
    DatabaseType databaseType = TypedSPILoader.getService(DatabaseType.class, "PostgreSQL");
    long start = System.nanoTime();
    List<ResponseHeader> result = new LinkedList<>();
    Map<String, DatabaseType> storageTypes = metaDataContexts.getMetaData().getDatabase(connectionSession.getDatabaseName()).getResourceMetaData().getStorageTypes();

    JDBCExecutorCallback<List<ExecuteResult>> callback = new BatchedJDBCExecutorCallback(storageTypes, sqlStatementSample, isExceptionThrown);
    try {
      List<List<ExecuteResult>> executeResults = jdbcExecutor.execute(executionGroupContext, callback);
      System.out.println("JDBC execution time: " + (System.nanoTime() - start) / 1000000 + "ms;");

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
            Long v = (Long) ((QueryResult) obj).getValue(1, Long.class);
            String tableName = meta.getTableName(1);
            if (obj instanceof JDBCMemoryQueryResult) {
              String sql = ((JDBCMemoryQueryResult) obj).sql;
              for (ExecutionGroup<JDBCExecutionUnit> group: executionGroupContext.getInputGroups()) {
                for (JDBCExecutionUnit unit: group.getInputs()) {
                  if (unit.getExecutionUnit().getSqlUnit().getSql().equals(sql)) {
                    long key = unit.getExecutionUnit().getKeys().get(tableName).get(0);
                    connectionSession.setValidationVersion(v, tableName, key);
                  }
                }
              }
//              List<SQLStatement> statements = parseSql(sql, databaseType);
//              if (statements.size() > 1) {
//                System.out.println("error statement size is large that 1, sql: " + sql);
//              }
//              ((JDBCMemoryQueryResult) obj).id = getKeyFromSQLStatementContext(statements.get(0));
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
      throw ex;
    }
    return result;
  }
}
