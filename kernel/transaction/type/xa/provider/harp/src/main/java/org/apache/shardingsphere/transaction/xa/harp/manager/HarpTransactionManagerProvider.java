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

package org.apache.shardingsphere.transaction.xa.harp.manager;

import com.atomikos.icatch.config.UserTransactionService;
import com.atomikos.icatch.config.UserTransactionServiceImp;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.shardingsphere.transaction.xa.spi.SingleXAResource;
import org.apache.shardingsphere.transaction.xa.spi.XATransactionManagerProvider;

import javax.sql.XADataSource;
import javax.transaction.SystemException;
import javax.transaction.RollbackException;

public class HarpTransactionManagerProvider implements XATransactionManagerProvider {
    
    @Getter
    HarpTransactionManager transactionManager;
    private UserTransactionService userTransactionService;
    
    @Override
    public void init() {
        transactionManager = new HarpTransactionManager();
        userTransactionService = new UserTransactionServiceImp();
        userTransactionService.init();
    }
    
    @Override
    public void registerRecoveryResource(String dataSourceName, XADataSource xaDataSource) {
        userTransactionService.registerResource(new HarpXARecoverableResource(dataSourceName, xaDataSource));
    }
    
    @Override
    public void removeRecoveryResource(String dataSourceName, XADataSource xaDataSource) {
        userTransactionService.removeResource(new HarpXARecoverableResource(dataSourceName, xaDataSource));
    }
    
    @SneakyThrows({SystemException.class, RollbackException.class})
    @Override
    public void enlistResource(SingleXAResource singleXAResource) {
        transactionManager.getTransaction().enlistResource(singleXAResource);
    }
    
    @Override
    public void close() throws Exception {
        userTransactionService.shutdown(true);
    }
    
    @Override
    public String getType() {
        return "Harp";
    }
    
    @Override
    public boolean isDefault() {
        return true;
    }
}
