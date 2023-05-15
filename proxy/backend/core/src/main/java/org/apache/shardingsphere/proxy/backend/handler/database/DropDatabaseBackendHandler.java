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

package org.apache.shardingsphere.proxy.backend.handler.database;

import com.google.common.base.Strings;
import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.authority.checker.AuthorityChecker;
import org.apache.shardingsphere.authority.rule.AuthorityRule;
import org.apache.shardingsphere.dialect.exception.syntax.database.DatabaseDropNotExistsException;
import org.apache.shardingsphere.dialect.exception.syntax.database.UnknownDatabaseException;
import org.apache.shardingsphere.infra.metadata.user.Grantee;
import org.apache.shardingsphere.infra.util.exception.ShardingSpherePreconditions;
import org.apache.shardingsphere.proxy.backend.context.ProxyContext;
import org.apache.shardingsphere.proxy.backend.handler.ProxyBackendHandler;
import org.apache.shardingsphere.proxy.backend.response.header.ResponseHeader;
import org.apache.shardingsphere.proxy.backend.response.header.update.UpdateResponseHeader;
import org.apache.shardingsphere.proxy.backend.session.ConnectionSession;
import org.apache.shardingsphere.sql.parser.sql.common.statement.ddl.DropDatabaseStatement;

import java.util.LinkedList;
import java.util.List;

/**
 * Drop database backend handler.
 */
@RequiredArgsConstructor
public final class DropDatabaseBackendHandler implements ProxyBackendHandler {
    
    private final DropDatabaseStatement sqlStatement;
    
    private final ConnectionSession connectionSession;
    
    @Override
    public List<ResponseHeader> execute() {
        check(sqlStatement, connectionSession.getGrantee());
        List<ResponseHeader> result = new LinkedList<ResponseHeader>();
        if (isDropCurrentDatabase(sqlStatement.getDatabaseName())) {
            connectionSession.setCurrentDatabase(null);
        }
        ProxyContext.getInstance().getContextManager().getInstanceContext().getModeContextManager().dropDatabase(sqlStatement.getDatabaseName());
        result.add(new UpdateResponseHeader(sqlStatement));
        return result;
    }
    
    private void check(final DropDatabaseStatement sqlStatement, final Grantee grantee) {
        String databaseName = sqlStatement.getDatabaseName().toLowerCase();
        AuthorityRule authorityRule = ProxyContext.getInstance().getContextManager().getMetaDataContexts().getMetaData().getGlobalRuleMetaData().getSingleRule(AuthorityRule.class);
        AuthorityChecker authorityChecker = new AuthorityChecker(authorityRule, grantee);
        ShardingSpherePreconditions.checkState(authorityChecker.isAuthorized(databaseName), () -> new UnknownDatabaseException(databaseName));
        ShardingSpherePreconditions.checkState(sqlStatement.isIfExists() || ProxyContext.getInstance().databaseExists(databaseName), () -> new DatabaseDropNotExistsException(databaseName));
    }
    
    private boolean isDropCurrentDatabase(final String databaseName) {
        return !Strings.isNullOrEmpty(connectionSession.getDatabaseName()) && connectionSession.getDatabaseName().equals(databaseName);
    }
}
