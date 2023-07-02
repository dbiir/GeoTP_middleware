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

import lombok.Getter;
import org.apache.shardingsphere.db.protocol.mysql.constant.MySQLConstants;
import org.apache.shardingsphere.db.protocol.mysql.packet.MySQLPacket;
import org.apache.shardingsphere.db.protocol.mysql.packet.command.admin.MySQLComSetOptionPacket;
import org.apache.shardingsphere.db.protocol.mysql.packet.command.query.text.MySQLTextResultSetRowPacket;
import org.apache.shardingsphere.db.protocol.mysql.packet.command.query.text.query.MySQLComQueryPacket;
import org.apache.shardingsphere.db.protocol.packet.DatabasePacket;
import org.apache.shardingsphere.infra.database.type.DatabaseType;
import org.apache.shardingsphere.infra.util.spi.type.typed.TypedSPILoader;
import org.apache.shardingsphere.mode.metadata.MetaDataContexts;
import org.apache.shardingsphere.parser.rule.SQLParserRule;
import org.apache.shardingsphere.proxy.backend.context.ProxyContext;
import org.apache.shardingsphere.proxy.backend.handler.ProxyBackendHandler;
import org.apache.shardingsphere.proxy.backend.handler.ProxyBackendHandlerFactory;
import org.apache.shardingsphere.proxy.backend.response.header.ResponseHeader;
import org.apache.shardingsphere.proxy.backend.response.header.query.QueryResponseHeader;
import org.apache.shardingsphere.proxy.backend.response.header.update.UpdateResponseHeader;
import org.apache.shardingsphere.proxy.backend.session.ConnectionSession;
import org.apache.shardingsphere.proxy.frontend.command.executor.QueryCommandExecutor;
import org.apache.shardingsphere.proxy.frontend.command.executor.ResponseType;
import org.apache.shardingsphere.proxy.frontend.mysql.command.ServerStatusFlagCalculator;
import org.apache.shardingsphere.proxy.frontend.mysql.command.query.builder.ResponsePacketBuilder;
import org.apache.shardingsphere.sql.parser.sql.common.statement.SQLStatement;
import org.apache.shardingsphere.sql.parser.sql.common.statement.dal.EmptyStatement;
import org.apache.shardingsphere.sql.parser.sql.common.statement.dml.DeleteStatement;
import org.apache.shardingsphere.sql.parser.sql.common.statement.dml.InsertStatement;
import org.apache.shardingsphere.sql.parser.sql.common.statement.dml.UpdateStatement;
import org.apache.shardingsphere.sql.parser.sql.common.util.SQLUtils;

import java.sql.SQLException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

/**
 * COM_QUERY command packet executor for MySQL.
 */
public final class MySQLComQueryPacketExecutor implements QueryCommandExecutor {
    
    private final ConnectionSession connectionSession;
    
    private final ProxyBackendHandler proxyBackendHandler;
    
    private final int characterSet;
    
    @Getter
    private volatile ResponseType responseType;
    
    public MySQLComQueryPacketExecutor(final MySQLComQueryPacket packet, final ConnectionSession connectionSession) throws SQLException {
        this.connectionSession = connectionSession;
        DatabaseType databaseType = TypedSPILoader.getService(DatabaseType.class, "MySQL");
        SQLStatement sqlStatement = parseSql1(packet.getSql(), databaseType);
        
        if (sqlStatement instanceof InsertStatement) {
            proxyBackendHandler = ProxyBackendHandlerFactory.newInstance(databaseType, packet.getSql(), sqlStatement, connectionSession, packet.getHintValueContext());
        } else {
            List<SQLStatement> sqlStatements = parseSql(packet.getSql(), databaseType);
            if (sqlStatements.size() <= 1) {
                proxyBackendHandler = ProxyBackendHandlerFactory.newInstance(databaseType, packet.getSql(), sqlStatements.get(0), connectionSession, packet.getHintValueContext());
            } else {
                proxyBackendHandler = new MySQLMultiStatementsHandler(connectionSession, sqlStatements, packet.getSql(), false);
            }
        }

        characterSet = connectionSession.getAttributeMap().attr(MySQLConstants.MYSQL_CHARACTER_SET_ATTRIBUTE_KEY).get().getId();
    }
    
    private SQLStatement parseSql1(final String sql, final DatabaseType databaseType) {
        if (SQLUtils.trimComment(sql).isEmpty()) {
            return new EmptyStatement();
        }
        MetaDataContexts metaDataContexts = ProxyContext.getInstance().getContextManager().getMetaDataContexts();
        SQLParserRule sqlParserRule = metaDataContexts.getMetaData().getGlobalRuleMetaData().getSingleRule(SQLParserRule.class);
        return sqlParserRule.getSQLParserEngine(databaseType.getType()).parse(sql, false);
    }
    
    private List<SQLStatement> parseSql(final String sql, final DatabaseType databaseType) {
        List<SQLStatement> result = new LinkedList<>();
        if (SQLUtils.trimComment(sql).isEmpty()) {
            result.add(new EmptyStatement());
            return result;
        }
        List<String> singleSqls = SQLUtils.splitMultiSQL(sql);
        if (singleSqls.size() < 1) {
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
    
    private boolean areMultiStatements(final ConnectionSession connectionSession, final SQLStatement sqlStatement, final String sql) {
        // TODO Multi statements should be identified by SQL Parser instead of checking if sql contains ";".
        return connectionSession.getAttributeMap().hasAttr(MySQLConstants.MYSQL_OPTION_MULTI_STATEMENTS)
                && MySQLComSetOptionPacket.MYSQL_OPTION_MULTI_STATEMENTS_ON == connectionSession.getAttributeMap().attr(MySQLConstants.MYSQL_OPTION_MULTI_STATEMENTS).get()
                && (sqlStatement instanceof UpdateStatement || sqlStatement instanceof DeleteStatement) && sql.contains(";");
    }
    
    @Override
    public Collection<DatabasePacket<?>> execute() throws SQLException {
        // TODO: multi executor response header
        Collection<DatabasePacket<?>> result = new LinkedList<>();
//        long startTime = System.currentTimeMillis();
        List<ResponseHeader> responseHeader = proxyBackendHandler.execute();
//        System.out.println("executeTime: " + (System.currentTimeMillis() - startTime) + " ms");
        
        for (ResponseHeader each : responseHeader) {
            if (each instanceof QueryResponseHeader) {
                result.addAll(processQuery((QueryResponseHeader) each));
                break;
            } else if (each instanceof UpdateResponseHeader) {
                responseType = ResponseType.UPDATE;
                result.addAll(processUpdate((UpdateResponseHeader) each));
                break;
            }
        }
        return result;
    }
    
    private Collection<DatabasePacket<?>> processQuery(final QueryResponseHeader queryResponseHeader) {
        responseType = ResponseType.QUERY;
        return ResponsePacketBuilder.buildQueryResponsePackets(queryResponseHeader, characterSet, ServerStatusFlagCalculator.calculateFor(connectionSession));
    }
    
    private Collection<DatabasePacket<?>> processUpdate(final UpdateResponseHeader updateResponseHeader) {
        return ResponsePacketBuilder.buildUpdateResponsePackets(updateResponseHeader, ServerStatusFlagCalculator.calculateFor(connectionSession));
    }
    
    @Override
    public boolean next() throws SQLException {
        return proxyBackendHandler.next();
    }
    
    @Override
    public MySQLPacket getQueryRowPacket() throws SQLException {
        return new MySQLTextResultSetRowPacket(proxyBackendHandler.getRowData().getData());
    }
    
    @Override
    public void close() throws SQLException {
        proxyBackendHandler.close();
    }
}
