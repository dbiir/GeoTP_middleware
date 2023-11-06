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

import com.atomikos.icatch.OrderedLifecycleComponent;
import com.atomikos.icatch.jta.TransactionManagerImp;
import com.atomikos.util.SerializableObjectFactory;
import com.atomikos.icatch.config.Configuration;
import lombok.Getter;
import lombok.Setter;

import javax.naming.NamingException;
import javax.naming.Reference;
import javax.naming.Referenceable;
import javax.transaction.*;
import java.io.Serializable;

@Getter
@Setter
public class HarpTransactionManager implements TransactionManager, Serializable, Referenceable, UserTransaction, OrderedLifecycleComponent {
    
    boolean rollbackOnly;
    // private transient TransactionManagerImp tm;
    private CustomTransactionManager tm;
    private boolean forceShutdown;
    private boolean startupTransactionService = true;
    private boolean closed = false;
    private boolean coreStartedHere;
    int transactionTimeout;
    
    private void checkSetup() throws SystemException {
        if (!this.closed) {
            this.initializeTransactionManagerSingleton();
        }
    }
    
    private void initializeTransactionManagerSingleton() throws SystemException {
        this.tm = (CustomTransactionManager) CustomTransactionManager.getTransactionManager();
        if (this.tm == null) {
            if (!this.getStartupTransactionService()) {
                throw new SystemException("Transaction service not running");
            }
            
            this.startupTransactionService();
            this.tm = (CustomTransactionManager) CustomTransactionManager.getTransactionManager();
        }
    }
    
    private void startupTransactionService() {
        this.coreStartedHere = Configuration.init();
    }
    
    public HarpTransactionManager() {
    }
    
    public void setStartupTransactionService(boolean startup) {
        this.startupTransactionService = startup;
    }
    
    public boolean getStartupTransactionService() {
        return this.startupTransactionService;
    }
    
    @Override
    public void begin() throws NotSupportedException, SystemException {
        if (this.closed) {
            throw new SystemException("This UserTransactionManager instance was closed already. Call init() to reuse if desired.");
        } else {
            this.checkSetup();
            // TODO:
            this.tm.begin();
        }
    }
    
    @Override
    public void commit() throws RollbackException, HeuristicMixedException, HeuristicRollbackException, SecurityException, IllegalStateException, SystemException {
        if (this.closed) {
            throw new SystemException("This UserTransactionManager instance was closed already - commit no longer allowed or possible.");
        } else {
            this.checkSetup();
            // TODO:
            this.tm.commit();
        }
    }
    
    @Override
    public int getStatus() throws SystemException {
        this.checkSetup();
        return this.tm.getStatus();
    }
    
    @Override
    public Transaction getTransaction() throws SystemException {
        this.checkSetup();
        return this.tm.getTransaction();
    }
    
    @Override
    public void resume(Transaction transaction) throws InvalidTransactionException, IllegalStateException, SystemException {
        this.checkSetup();
        this.tm.resume(transaction);
    }
    
    @Override
    public void rollback() throws IllegalStateException, SecurityException, SystemException {
        // TODO:
        this.tm.rollback();
    }
    
    @Override
    public void setRollbackOnly() throws IllegalStateException, SystemException {
        this.tm.setRollbackOnly();
    }
    
    @Override
    public Transaction suspend() throws SystemException {
        assert false;
        this.checkSetup();
        return this.tm.suspend();
    }
    
    @Override
    public Reference getReference() throws NamingException {
        return SerializableObjectFactory.createReference(this);
    }
    
    @Override
    public void init() throws SystemException {
        this.closed = false;
        this.checkSetup();
    }
    
    @Override
    public void close() throws Exception {
        this.shutdownTransactionService();
        this.closed = true;
    }
    
    public void setTransactionTimeout(int secs) throws SystemException {
        this.checkSetup();
        this.tm.setTransactionTimeout(secs);
    }
    
    private void shutdownTransactionService() {
        if (this.coreStartedHere) {
            Configuration.shutdown(this.forceShutdown);
            this.coreStartedHere = false;
        }
        
    }
}
