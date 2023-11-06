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

import com.atomikos.datasource.ResourceException;
import com.atomikos.datasource.ResourceTransaction;
import com.atomikos.datasource.xa.XAExceptionHelper;
import com.atomikos.datasource.xa.XATransactionalResource;
import com.atomikos.datasource.xa.XID;
import com.atomikos.icatch.SysException;
import com.atomikos.recovery.TxState;
import com.atomikos.util.Assert;
import com.mysql.jdbc.jdbc2.optional.MysqlXid;
import lombok.extern.slf4j.Slf4j;
import org.apache.shardingsphere.infra.transactions.CustomXID;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.util.Arrays;
import java.util.Map;

@Slf4j
public class XAResourceTransaction implements ResourceTransaction {
    
    static final long serialVersionUID = -8227293322090019196L;
    private String tid;
    private String root;
    private boolean isXaSuspended;
    private TxState state;
    private String resourcename;
    private transient Xid xid;
    private transient String toString;
    private final transient XATransactionalResource resource;
    private transient XAResource xaresource;
    private transient boolean knownInResource;
    private transient int timeout;
    
    private static String interpretErrorCode(String resourceName, String opCode, Xid xid, int errorCode) {
        String msg = XAExceptionHelper.convertErrorCodeToVerboseMessage(errorCode);
        return "XA resource '" + resourceName + "': " + opCode + " for XID '" + xid + "' raised " + errorCode + ": " + msg;
    }
    
    private void setXid(Xid xid) {
        this.xid = xid;
        this.toString = "XAResourceTransaction: " + xid;
    }
    
    XAResourceTransaction(XATransactionalResource resource, String tid, String root) {
        assert resource != null;
        this.resource = resource;
        this.timeout = 200;
        this.tid = tid;
        this.root = root;
        this.resourcename = resource.getName();
        MysqlXid mysqlXid = new MysqlXid(tid.getBytes(), resourcename.getBytes(), 1);
        this.setXid(mysqlXid);
        this.setState(TxState.ACTIVE);
        this.isXaSuspended = false;
        this.knownInResource = false;
    }
    
    public int hashCode() {
        return this.xid.hashCode();
    }
    
    public String toString() {
        return this.toString;
    }
    
    void setState(TxState state) {
        if (state.isHeuristic()) {
            log.warn("Heuristic termination of " + this.toString() + " with state " + state);
        }
        
        this.state = state;
    }
    
    public Xid getXid() {
        return xid;
    }
    
    public void setXAResource(XAResource xaresource) {
        if (log.isTraceEnabled()) {
            log.trace(this + ": about to switch to XAResource " + xaresource);
        }
        
        this.xaresource = xaresource;
        
        try {
            this.xaresource.setTransactionTimeout(this.timeout);
        } catch (XAException var4) {
            String msg = interpretErrorCode(this.resourcename, "setTransactionTimeout", this.xid, var4.errorCode);
            log.warn(msg, var4);
        }
        
        if (log.isTraceEnabled()) {
            log.trace("XAResourceTransaction " + this.getXid() + ": switched to XAResource " + xaresource);
        }
        
    }
    
    public synchronized void suspend() throws ResourceException {
        if (this.state.equals(TxState.ACTIVE)) {
            try {
                log.debug("XAResource.end ( " + this.xid + " , XAResource.TMSUCCESS ) on resource " + this.resourcename + " represented by XAResource instance " + this.xaresource);
                this.xaresource.end(this.xid, 67108864);
            } catch (XAException var3) {
                String msg = interpretErrorCode(this.resourcename, "end", this.xid, var3.errorCode);
                log.trace(msg, var3);
            }
            
            this.setState(TxState.LOCALLY_DONE);
        }
    }
    
    @Override
    public synchronized void resume() throws IllegalStateException, ResourceException {
        int flag;
        String logFlag = "";
        if (this.state.equals(TxState.LOCALLY_DONE)) {
            flag = 2097152;
            logFlag = "XAResource.TMJOIN";
        } else {
            if (this.knownInResource) {
                throw new IllegalStateException("Wrong state for resume: " + this.state);
            }
            
            flag = 0;
            logFlag = "XAResource.TMNOFLAGS";
        }
        
        try {
            if (log.isDebugEnabled()) {
                log.debug("XAResource.start ( " + this.xid + " , " + logFlag + " ) on resource " + this.resourcename + " represented by XAResource instance " + this.xaresource);
            }
            
            this.xaresource.start(this.xid, flag);
        } catch (XAException var5) {
            String msg = interpretErrorCode(this.resourcename, "resume", this.xid, var5.errorCode);
            log.warn(msg, var5);
            throw new ResourceException(msg, var5);
        }
        
        this.setState(TxState.ACTIVE);
        this.knownInResource = true;
    }
    
    public void xaSuspend() throws XAException {
        if (!this.isXaSuspended) {
            try {
                if (log.isDebugEnabled()) {
                    log.debug("XAResource.suspend ( " + this.xid + " , XAResource.TMSUSPEND ) on resource " + this.resourcename + " represented by XAResource instance " + this.xaresource);
                }
                
                this.xaresource.end(this.xid, 33554432);
                this.isXaSuspended = true;
            } catch (XAException var3) {
                String msg = interpretErrorCode(this.resourcename, "suspend", this.xid, var3.errorCode);
                log.warn(msg, var3);
                throw var3;
            }
        }
        
    }
    
    public void xaResume() throws XAException {
        try {
            if (log.isDebugEnabled()) {
                log.debug("XAResource.start ( " + this.xid + " , XAResource.TMRESUME ) on resource " + this.resourcename + " represented by XAResource instance " + this.xaresource);
            }
            
            this.xaresource.start(this.xid, 134217728);
            this.isXaSuspended = false;
        } catch (XAException var3) {
            String msg = interpretErrorCode(this.resourcename, "resume", this.xid, var3.errorCode);
            log.warn(msg, var3);
            throw var3;
        }
    }
    
    public boolean isXaSuspended() {
        return this.isXaSuspended;
    }
    
    public boolean isActive() {
        return this.state.equals(TxState.ACTIVE);
    }
    
    public String getURI() {
        return Arrays.toString(this.xid.getBranchQualifier());
    }
    
    public String getResourceName() {
        return this.resourcename;
    }
    
    private void assertConnectionIsStillAlive() throws XAException {
        this.xaresource.isSameRM(this.xaresource);
    }
}
