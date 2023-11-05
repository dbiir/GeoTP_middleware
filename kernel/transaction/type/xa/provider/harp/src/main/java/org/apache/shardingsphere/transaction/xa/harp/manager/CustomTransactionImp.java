package org.apache.shardingsphere.transaction.xa.harp.manager;

import com.atomikos.datasource.RecoverableResource;
import com.atomikos.datasource.ResourceException;
import com.atomikos.datasource.TransactionalResource;
//import com.atomikos.datasource.xa.XAResourceTransaction;
import com.atomikos.datasource.xa.XATransactionalResource;
import com.atomikos.icatch.config.Configuration;
import com.atomikos.icatch.jta.ExtendedSystemException;
import com.mysql.jdbc.StringUtils;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.shardingsphere.infra.statistics.network.Latency;
import org.apache.shardingsphere.infra.transactions.AgentAsyncXAManager;
import org.apache.shardingsphere.infra.transactions.CustomXID;
import org.apache.shardingsphere.infra.transactions.XATransactionState;
import org.apache.shardingsphere.sql.parser.sql.common.util.SQLUtils;

import javax.transaction.*;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.util.*;

@Slf4j
public class CustomTransactionImp implements Transaction {
    @Getter
    private final String tid;
    private final Map<XAResourceKey, XAResourceTransaction> xaResourceToResourceTransactionMap_;
    @Getter
    private final Scheduler<Synchronization> synchronizationScheduler = new Scheduler();

    private volatile Date startDate;
    private volatile Date timeoutDate;

    @Setter
    private volatile int status = 6;
    private volatile boolean timeout = false;

    CustomTransactionImp(String tid) {
        this.xaResourceToResourceTransactionMap_ = new HashMap<>();
        this.tid = tid;
    }

    private synchronized void addXAResourceTransaction(XAResourceTransaction restx, XAResource xares) {
        this.xaResourceToResourceTransactionMap_.put(new XAResourceKey(xares), restx);
    }

    private synchronized void removeXAResourceTransaction(XAResource xares) {
        this.xaResourceToResourceTransactionMap_.remove(new XAResourceKey(xares));
    }

    private static void rethrowAsJtaRollbackException(String msg, Throwable cause) throws RollbackException {
        RollbackException ret = new RollbackException(msg);
        ret.initCause(cause);
        throw ret;
    }

    private static void rethrowAsJtaHeuristicMixedException(String msg, Throwable cause) throws HeuristicMixedException {
        HeuristicMixedException ret = new HeuristicMixedException(msg);
        ret.initCause(cause);
        throw ret;
    }

    private static void rethrowAsJtaHeuristicRollbackException(String msg, Throwable cause) throws HeuristicRollbackException {
        HeuristicRollbackException ret = new HeuristicRollbackException(msg);
        ret.initCause(cause);
        throw ret;
    }

    @Override
    public void commit() throws RollbackException, SecurityException, IllegalStateException {
        // TODO
        boolean onePhase = xaResourceToResourceTransactionMap_.size() == 1;
        if (Latency.getInstance().asyncPreparation()) {
            try {
                asyncCommit(onePhase);
            } catch (InterruptedException e) {
                throw new RuntimeException("Async Commit Failed." + e);
            } catch (XAException e) {
                throw new RollbackException(e.toString());
            }
        } else {
            try {
                syncCommit(onePhase);
            } catch (XAException e) {
                throw new RollbackException(e.toString());
            }
        }
    }

    public void syncCommit(boolean onePhase) throws XAException {
        boolean prepareSuccess = true;
        long enterTime = System.nanoTime();
        System.out.println("Enter in commit: " + enterTime);
        // xa end
        try {
            for(XAResourceKey each: xaResourceToResourceTransactionMap_.keySet()) {
                XAResourceTransaction txn = xaResourceToResourceTransactionMap_.get(each);
                each.getXares().end(txn.getXid(), 67108864);
            }
        } catch (XAException e) {
            log.error("xa end failed.");
            throw e;
        }
        long xaEndTime = System.nanoTime();
        System.out.println("XA End Time: " + (xaEndTime - enterTime) / 1000);

        // xa prepare
        if (!onePhase) {
            for(XAResourceKey each: xaResourceToResourceTransactionMap_.keySet()) {
                XAResourceTransaction txn = xaResourceToResourceTransactionMap_.get(each);
                try {
                    each.getXares().prepare(txn.getXid());
                } catch (XAException ex) {
                    prepareSuccess = false;
                }
            }
        }

        long xaPrepareTime = System.nanoTime();
        System.out.println("XA Prepare Time: " + (xaPrepareTime - xaEndTime) / 1000);

        if (prepareSuccess) {
            for(XAResourceKey each: xaResourceToResourceTransactionMap_.keySet()) {
                XAResourceTransaction txn = xaResourceToResourceTransactionMap_.get(each);
                if (onePhase) {
                    assert xaResourceToResourceTransactionMap_.size() == 1;
                    each.getXares().commit(txn.getXid(), true);
                } else {
                    each.getXares().commit(txn.getXid(), false);
                }
            }
        } else {
            for(XAResourceKey each: xaResourceToResourceTransactionMap_.keySet()) {
                XAResourceTransaction txn = xaResourceToResourceTransactionMap_.get(each);
                each.getXares().rollback(txn.getXid());
            }
        }

        long xaCommitTime = System.nanoTime();
        System.out.println("XA Commit Time: " + (xaCommitTime - xaPrepareTime) / 1000);
    }

    public void asyncCommit(boolean onePhase) throws InterruptedException, XAException {
        boolean prepareSuccess = true;

        if (onePhase) {
            long enterTime = System.nanoTime();
            System.out.println("Enter in commit: " + enterTime);
            for(XAResourceKey each: xaResourceToResourceTransactionMap_.keySet()) {
                CustomXID xid = new CustomXID(SQLUtils.xidToHex(xaResourceToResourceTransactionMap_.get(each).getXid()));
                while (AgentAsyncXAManager.getInstance().getStateByXid(xid) != XATransactionState.IDLE &&
                        AgentAsyncXAManager.getInstance().getStateByXid(xid) != XATransactionState.ROLLBACK_ONLY
                ) {
                    Thread.sleep(1);
                }
                if (AgentAsyncXAManager.getInstance().getStateByXid(xid) == XATransactionState.ROLLBACK_ONLY) {
                    prepareSuccess = false;
                }
            }
            long finishAsyncTime = System.nanoTime();
            System.out.println("Txn " + tid + " Wait Async Commit Time: " + (finishAsyncTime - enterTime) / 1000 + "us");
            try {
                for(XAResourceKey each: xaResourceToResourceTransactionMap_.keySet()) {
                    XAResourceTransaction txn = xaResourceToResourceTransactionMap_.get(each);
                    if (prepareSuccess) {
                        each.getXares().commit(txn.getXid(), true);
                    } else {
                        each.getXares().rollback(txn.getXid());
                    }
                }
            } catch (XAException e) {
                if (prepareSuccess) {
                    log.error("commit one phase failed.");
                } else {
                    log.error("rollback only failed.");
                }
                throw e;
            }
            long endTime = System.nanoTime();
            System.out.println("Txn " + tid + " Finish Commit Time: " + (endTime - finishAsyncTime) / 1000 + "us");
        } else {
            long enterTime = System.nanoTime();
            System.out.println("Enter in commit: " + enterTime);
            for(XAResourceKey each: xaResourceToResourceTransactionMap_.keySet()) {
                CustomXID xid = new CustomXID(SQLUtils.xidToHex(xaResourceToResourceTransactionMap_.get(each).getXid()));
                while (AgentAsyncXAManager.getInstance().getStateByXid(xid) != XATransactionState.PREPARED &&
                        AgentAsyncXAManager.getInstance().getStateByXid(xid) != XATransactionState.FAILED &&
                        AgentAsyncXAManager.getInstance().getStateByXid(xid) != XATransactionState.ROLLBACK_ONLY
                ) {
                    Thread.sleep(1);
                }
                if (AgentAsyncXAManager.getInstance().getStateByXid(xid) == XATransactionState.ROLLBACK_ONLY ||
                        AgentAsyncXAManager.getInstance().getStateByXid(xid) == XATransactionState.FAILED
                ) {
                    prepareSuccess = false;
                }
            }
            long finishAsyncTime = System.nanoTime();
            System.out.println("Txn " + tid + " Wait Async Commit Time: " + (finishAsyncTime - enterTime) / 1000 + "us");
            List<Thread> threadList = new LinkedList<>();
            for(XAResourceKey each: xaResourceToResourceTransactionMap_.keySet()) {
                boolean finalPrepareSuccess = prepareSuccess;
                threadList.add(new Thread(() -> {
                    XAResourceTransaction txn = xaResourceToResourceTransactionMap_.get(each);
                    try {
                        if (finalPrepareSuccess) {
                            each.getXares().commit(txn.getXid(), false);
                        } else {
                            each.getXares().rollback(txn.getXid());
                        }
                    } catch (XAException ex) {
                        if (finalPrepareSuccess) {
                            log.info("commit one phase failed.");
                        } else {
                            log.info("rollback only failed.");
                        }
                        throw new RuntimeException(ex.toString());
                    }
                }));
            }
            for (Thread each: threadList) {
                each.start();
            }
            for (Thread each: threadList) {
                each.join();
            }
            long endTime = System.nanoTime();
            System.out.println("Txn " + tid + " Finish Commit Time: " + (endTime - finishAsyncTime) / 1000 + "us");
        }

    }

    @Override
    public boolean delistResource(XAResource xares, int flag) throws IllegalStateException, SystemException {
        log.debug("delistResource ( " + xares + " ) with transaction " + this.toString());

        XAResourceTransaction active = this.findXAResourceTransaction(xares);
        String msg;
        if (active == null) {
            msg = "Illegal attempt to delist an XAResource instance that was not previously enlisted.";
            log.warn(msg);
            throw new IllegalStateException(msg);
        } else {
            if (flag != 67108864 && flag != 536870912) {
                if (flag != 33554432) {
                    msg = "Unknown delist flag: " + flag;
                    log.warn(msg);
                    throw new SystemException(msg);
                }

                try {
                    active.xaSuspend();
                } catch (XAException var5) {
                    throw new ExtendedSystemException("Error in delisting the given XAResource", var5);
                }
            } else {
                try {
                    active.suspend();
                } catch (ResourceException var6) {
                    throw new ExtendedSystemException("Error in delisting the given XAResource", var6);
                }

                this.removeXAResourceTransaction(xares);
                if (flag == 536870912) {
                    this.setRollbackOnly();
                }
            }

            return true;
        }
    }

    @Override
    public boolean enlistResource(XAResource xares) throws RollbackException, IllegalStateException, SystemException {
        TransactionalResource res = null;
        XAResourceTransaction restx = null;
        int status = this.getStatus();
        String msg;
        switch (status) {
            case 1:
            case 4:
            case 9:
                msg = "Transaction rollback - enlisting more resources is useless.";
                log.warn(msg);
                throw new RollbackException(msg);
            case 2:
            case 3:
            case 5:
                msg = "Enlisting more resources is no longer permitted: transaction is in state " + this.status;
                log.warn(msg);
                throw new IllegalStateException(msg);
            case 6:
            case 7:
            case 8:
            default:
                XAResourceTransaction suspendedXAResourceTransaction = this.findXAResourceTransaction(xares);
                if (suspendedXAResourceTransaction != null) {
                    if (!suspendedXAResourceTransaction.isXaSuspended()) {
                        msg = "The given XAResource instance is being enlisted a second time without delist in between?";
                        log.warn(msg);
                        throw new IllegalStateException(msg);
                    }

                    try {
                        suspendedXAResourceTransaction.setXAResource(xares);
                        suspendedXAResourceTransaction.xaResume();
                    } catch (XAException var9) {
                        if (100 <= var9.errorCode && var9.errorCode <= 107) {
                            rethrowAsJtaRollbackException("Transaction was already rolled back inside the back-end resource. Further enlists are useless.", var9);
                        }

                        throw new ExtendedSystemException("Unexpected error during enlist", var9);
                    }
                } else {
                    res = this.findRecoverableResourceForXaResource(xares);

                    if (res == null) {
                        msg = "There is no registered resource that can recover the given XAResource instance. \nPlease register a corresponding resource first.";
                        log.warn(msg);
                        throw new SystemException(msg);
                    }

                    try {
                        restx = new XAResourceTransaction((XATransactionalResource) res, tid, tid);
                        restx.setXAResource(xares);
                        restx.resume();
                    } catch (ResourceException var7) {
                        throw new ExtendedSystemException("Unexpected error during enlist", var7);
                    } catch (RuntimeException var8) {
                        throw var8;
                    }

                    this.addXAResourceTransaction(restx, xares);
                }

                assert restx != null;
                registerIntoAsyncXAManager(restx.getXid());
                return true;
        }
    }

    private void registerIntoAsyncXAManager(Xid xid) {
        CustomXID test = new CustomXID(SQLUtils.xidToHex(xid));

        AgentAsyncXAManager.getInstance().setStateByXid(test, XATransactionState.ACTIVE);
    }

    private synchronized XAResourceTransaction findXAResourceTransaction(XAResource xares) {
        XAResourceTransaction ret = null;
        ret = (XAResourceTransaction)this.xaResourceToResourceTransactionMap_.get(new XAResourceKey(xares));
        if (ret != null) {
            this.assertActiveOrSuspended(ret);
        }

        return ret;
    }

    private void assertActiveOrSuspended(XAResourceTransaction restx) {
        if (!restx.isActive() && !restx.isXaSuspended()) {
            log.warn("Unexpected resource transaction state for " + restx);
        }

    }

    private TransactionalResource findRecoverableResourceForXaResource(XAResource xares) {
        TransactionalResource ret = null;
        synchronized(Configuration.class) {
            Collection<RecoverableResource> resources = Configuration.getResources();

            for (RecoverableResource rres : resources) {
                if (rres instanceof XATransactionalResource) {
                    XATransactionalResource xatxres = (XATransactionalResource) rres;
                    if (xatxres.usesXAResource(xares)) {
                        ret = xatxres;
                    }
                }
            }

            return ret;
        }
    }

    @Override
    public int getStatus() throws SystemException {
        return this.status;
    }

    public void setActive(int timeout) throws IllegalStateException, SystemException {
        if (this.status != 6) {
            throw new IllegalStateException("transaction has already started");
        } else {
            this.setStatus(0);
            this.startDate = new Date(System.currentTimeMillis());
            this.timeoutDate = new Date(System.currentTimeMillis() + (long)timeout * 1000L);
        }
    }

    @Override
    public void registerSynchronization(Synchronization synchronization) throws RollbackException, IllegalStateException, SystemException {
        if (this.status == 6) {
            throw new IllegalStateException("transaction hasn't started yet");
        } else if (this.status == 1) {
            throw new RollbackException("transaction has been marked as rollback only");
        } else if (this.isDone()) {
            throw new IllegalStateException("transaction is done, cannot register any more synchronization");
        } else {
            if (log.isDebugEnabled()) {
                log.debug("registering synchronization " + synchronization);
            }

            this.synchronizationScheduler.add(synchronization, Scheduler.DEFAULT_POSITION);
        }
    }

    @SneakyThrows
    @Override
    public void rollback() throws IllegalStateException, SystemException {
        if (Latency.getInstance().asyncPreparation()) {
            asyncRollback();
        } else {
            syncRollback();
        }
    }

    public void syncRollback() throws XAException {
        // xa end
        try {
            for(XAResourceKey each: xaResourceToResourceTransactionMap_.keySet()) {
                XAResourceTransaction txn = xaResourceToResourceTransactionMap_.get(each);
                each.getXares().end(txn.getXid(), 67108864);
            }
        } catch (XAException e) {
            log.error("xa end failed.");
            throw e;
        }

        // xa rollback
        for(XAResourceKey each: xaResourceToResourceTransactionMap_.keySet()) {
            XAResourceTransaction txn = xaResourceToResourceTransactionMap_.get(each);
            each.getXares().rollback(txn.getXid());
        }
    }

    public void asyncRollback() throws InterruptedException, XAException {
        for(XAResourceKey each: xaResourceToResourceTransactionMap_.keySet()) {
            CustomXID xid = new CustomXID(SQLUtils.xidToHex(xaResourceToResourceTransactionMap_.get(each).getXid()));
            while (AgentAsyncXAManager.getInstance().getStateByXid(xid) != XATransactionState.PREPARED &&
                    AgentAsyncXAManager.getInstance().getStateByXid(xid) != XATransactionState.FAILED &&
                    AgentAsyncXAManager.getInstance().getStateByXid(xid) != XATransactionState.ROLLBACK_ONLY &&
                    AgentAsyncXAManager.getInstance().getStateByXid(xid) != XATransactionState.IDLE
            ) {
                Thread.sleep(1);
            }
        }
        List<Thread> threadList = new LinkedList<>();
        for(XAResourceKey each: xaResourceToResourceTransactionMap_.keySet()) {
            threadList.add(new Thread(() -> {
                XAResourceTransaction txn = xaResourceToResourceTransactionMap_.get(each);
                try {
                    each.getXares().rollback(txn.getXid());
                } catch (XAException ex) {
                    log.error("rollback failed.");
                    throw new RuntimeException(ex.toString());
                }
            }));
        }
        for (Thread each: threadList) {
            each.start();
        }
        for (Thread each: threadList) {
            each.join();
        }
    }


    @Override
    public void setRollbackOnly() throws IllegalStateException, SystemException {
        if (this.status == 6) {
            throw new IllegalStateException("transaction hasn't started yet");
        } else if (this.isDone()) {
            throw new IllegalStateException("transaction is done, cannot change its status");
        } else {
            this.setStatus(1);
        }
    }

    public boolean equals(Object o) {
        if (o instanceof CustomTransactionImp) {
            CustomTransactionImp other = (CustomTransactionImp)o;
            return this.tid.equals(other.tid);
        } else {
            return false;
        }
    }

    public int hashCode() {
        return this.tid.hashCode();
    }

    public String toString() {
        return "a Custom Transaction with Tid [" + this.tid + "], status=" + Decoder.decodeStatus(this.status);
    }


    void suspendEnlistedXaResources() throws ExtendedSystemException {

        for (XAResourceTransaction resTx : this.xaResourceToResourceTransactionMap_.values()) {
            try {
                resTx.xaSuspend();
            } catch (XAException var4) {
                throw new ExtendedSystemException("Error in suspending the given XAResource", var4);
            }
        }

    }


    void resumeEnlistedXaReources() throws ExtendedSystemException {
        Iterator<XAResourceTransaction> xaResourceTransactions = this.xaResourceToResourceTransactionMap_.values().iterator();

        while(xaResourceTransactions.hasNext()) {
            XAResourceTransaction resTx = xaResourceTransactions.next();

            try {
                resTx.xaResume();
                xaResourceTransactions.remove();
            } catch (XAException var4) {
                throw new ExtendedSystemException("Error in resuming the given XAResource", var4);
            }
        }
    }

    private boolean isDone() {
        switch (this.status) {
            case 2:
            case 3:
            case 4:
            case 7:
            case 8:
            case 9:
                return true;
            case 5:
            case 6:
            default:
                return false;
        }
    }

    private boolean isWorking() {
        switch (this.status) {
            case 2:
            case 7:
            case 8:
            case 9:
                return true;
            case 3:
            case 4:
            case 5:
            case 6:
            default:
                return false;
        }
    }
}
