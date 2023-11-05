package org.apache.shardingsphere.transaction.xa.harp.manager;

import com.atomikos.icatch.SysException;
import com.atomikos.icatch.jta.ExtendedSystemException;
import com.atomikos.icatch.jta.TransactionManagerFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.shardingsphere.infra.transactions.AgentAsyncXAManager;

import javax.naming.Reference;
import javax.naming.Referenceable;
import javax.naming.StringRefAddr;
import javax.transaction.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class CustomTransactionManager implements TransactionManager, Referenceable, UserTransaction {
    private static final CustomTransactionManager singleton = new CustomTransactionManager();
    private final Map<String, CustomTransactionImp> jtaTransactionToCoreTransactionMap; // Tid -> CustomTransactionImp
    private final Map<Thread, ThreadTransactionContext> contexts;

    private static void raiseNoTransaction() {
        StringBuilder msg = new StringBuilder();
        msg.append("This method needs a transaction for the calling thread and none exists.\n");
        msg.append("Possible causes: either you didn't start a transaction,\n");
        msg.append("it rolledback due to timeout, or it was committed already.\n");
        msg.append("ACTIONS: You can try one of the following: \n");
        msg.append("1. Make sure you started a transaction for the thread.\n");
        msg.append("2. Make sure you didn't terminate it yet.\n");
        msg.append("3. Increase the transaction timeout to avoid automatic rollback of long transactions;\n");
        log.warn(msg.toString());
        throw new IllegalStateException(msg.toString());
    }

    public static TransactionManager getTransactionManager() {
        return singleton;
    }

    private CustomTransactionManager() {
        this.jtaTransactionToCoreTransactionMap = new ConcurrentHashMap<>(128, 0.75F, 128);
        this.contexts = new ConcurrentHashMap<>(128, 0.75F, 128);
    }

    private void addToMap(String tid, CustomTransactionImp tx) {
        synchronized(this.jtaTransactionToCoreTransactionMap) {
            this.jtaTransactionToCoreTransactionMap.put(tid, tx);
        }
    }

    private void removeFromMap(String tid) {
        synchronized(this.jtaTransactionToCoreTransactionMap) {
            this.jtaTransactionToCoreTransactionMap.remove(tid);
        }
    }

    CustomTransactionImp getTransactionWithId(String tid) {
        synchronized(this.jtaTransactionToCoreTransactionMap) {
            return this.jtaTransactionToCoreTransactionMap.get(tid);
        }
    }

    public Transaction getTransaction(String tid) {
        return this.getTransactionWithId(tid);
    }

    public Transaction getTransaction() {
        return this.getCurrentTransaction();
    }

    public CustomTransactionImp getCurrentTransaction() {
        return this.contexts.get(Thread.currentThread()) == null ? null : this.getOrCreateCurrentContext().getTransaction();
    }

    private ThreadTransactionContext getOrCreateCurrentContext() {
        ThreadTransactionContext threadContext = this.contexts.get(Thread.currentThread());
        if (threadContext == null) {
            if (log.isDebugEnabled()) {
                log.debug("creating new thread context");
            }

            threadContext = new ThreadTransactionContext();
            this.setCurrentContext(threadContext);
        }

        return threadContext;
    }

    private void setCurrentContext(ThreadTransactionContext context) {
        if (log.isDebugEnabled()) {
            log.debug("changing current thread context to " + context);
        }

        if (context == null) {
            throw new IllegalArgumentException("setCurrentContext() should not be called with a null context, clearCurrentContextForSuspension() should be used instead");
        } else {
            this.contexts.put(Thread.currentThread(), context);
        }
    }

    private CustomTransactionImp establishTransaction(String tid) {
        CustomTransactionImp transaction = this.getTransactionWithId(tid);
        if (transaction == null) {
            return this.createTransaction(tid);
        }
        return transaction;
    }

    private CustomTransactionImp createTransaction(String tid) {
        CustomTransactionImp ret = new CustomTransactionImp(tid);
        this.addToMap(tid, ret);
        return ret;
    }

    public void committed(String tid) {
        this.removeFromMap(tid);
    }

    public void rolledback(String tid) {
        this.removeFromMap(tid);
    }

    @Override
    public Reference getReference() {
        return new Reference(this.getClass().getName(), new StringRefAddr("name", "TransactionManager"), TransactionManagerFactory.class.getName(), null);
    }

    @Override
    public void begin() throws SystemException, NotSupportedException {
        CustomTransactionImp currentTx = this.getCurrentTransaction();
        if (currentTx  != null) {
            throw new NotSupportedException("nested transactions not supported");
        }
        String tid = String.valueOf(AgentAsyncXAManager.getInstance().fetchAndAddGlobalTid());
        currentTx = this.establishTransaction(tid);
        this.getOrCreateCurrentContext().setTransaction(currentTx);

        ClearContextSynchronization clearContextSynchronization = new ClearContextSynchronization(currentTx);

        try {
            currentTx.getSynchronizationScheduler().add(clearContextSynchronization, Scheduler.ALWAYS_LAST_POSITION - 1);
            currentTx.setActive(this.getOrCreateCurrentContext().getTimeout());
        } catch (RuntimeException | SystemException ex) {
            clearContextSynchronization.afterCompletion(6);
            throw ex;
        }
    }

    @Override
    public void commit() throws RollbackException, HeuristicMixedException, HeuristicRollbackException, SecurityException, IllegalStateException, SystemException {
        Transaction tx = this.getTransaction();
        if (tx == null) {
            raiseNoTransaction();
        }

        assert tx != null;
        ClearContextSynchronization clearContextSynchronization = new ClearContextSynchronization((CustomTransactionImp)tx);
        try {
            tx.commit();
            clearContextSynchronization.afterCompletion(3);
            committed(((CustomTransactionImp)tx).getTid());
        } catch (RollbackException | HeuristicMixedException | HeuristicRollbackException | SecurityException | IllegalStateException | SystemException ex) {
            clearContextSynchronization.afterCompletion(4);
            rolledback(((CustomTransactionImp)tx).getTid());
            throw ex;
        }
    }

    @Override
    public int getStatus() throws SystemException {
        Transaction tx = this.getTransaction();
        int ret;
        if (tx == null) {
            ret = 6;
        } else {
            ret = tx.getStatus();
        }

        return ret;
    }

    @Override
    public void resume(Transaction transaction) throws InvalidTransactionException, IllegalStateException, SystemException {
        if (transaction instanceof CustomTransactionImp) {
            CustomTransactionImp ret = (CustomTransactionImp)transaction;

            try {
//                this.compositeTransactionManager.resume(tximp.getCT());
            } catch (SysException var5) {
                String msg = "Unexpected error while resuming the transaction in the calling thread";
                log.error(msg, var5);
                throw new ExtendedSystemException(msg, var5);
            }

            ret.resumeEnlistedXaReources();
        } else {
            String msg = "The specified transaction object is invalid for this configuration: " + transaction;
            log.error(msg);
            throw new InvalidTransactionException(msg);
        }
    }

    @Override
    public void rollback() throws IllegalStateException, SecurityException, SystemException {
        Transaction tx = this.getTransaction();
        if (tx == null) {
            raiseNoTransaction();
        }

        assert tx != null;
        ClearContextSynchronization clearContextSynchronization = new ClearContextSynchronization((CustomTransactionImp)tx);

        try {
            tx.rollback();
            rolledback(((CustomTransactionImp)tx).getTid());
        } catch (SecurityException | IllegalStateException | SystemException ex) {
            throw ex;
        } finally {
            clearContextSynchronization.afterCompletion(4);
        }
    }

    @Override
    public void setRollbackOnly() throws IllegalStateException, SystemException {
        Transaction tx = this.getTransaction();
        if (tx == null) {
            raiseNoTransaction();
        }

        try {
            assert tx != null;
            tx.setRollbackOnly();
        } catch (SecurityException var4) {
            String msg = "Unexpected error during setRollbackOnly";
            log.error(msg, var4);
            throw new ExtendedSystemException(msg, var4);
        }
    }

    @Override
    public void setTransactionTimeout(int seconds) throws SystemException {
        if (seconds >= 0) {
            this.getOrCreateCurrentContext().setTimeout(seconds);
        } else {
            assert false;
        }
    }

    @Override
    public Transaction suspend() throws SystemException {
        CustomTransactionImp ret = (CustomTransactionImp)this.getTransaction();
        if (ret != null) {
            ret.suspendEnlistedXaResources();
            this.clearCurrentContextForSuspension();
        }

        return ret;
    }

    private void clearCurrentContextForSuspension() {
        if (log.isDebugEnabled()) {
            log.debug("clearing current thread context: " + this.getOrCreateCurrentContext());
        }

        this.contexts.remove(Thread.currentThread());
        if (log.isDebugEnabled()) {
            log.debug("cleared current thread context: " + this.getOrCreateCurrentContext());
        }
    }

    private class ClearContextSynchronization implements Synchronization {
        private final CustomTransactionImp currentTx;

        public ClearContextSynchronization(CustomTransactionImp currentTx) {
            this.currentTx = currentTx;
        }

        public void beforeCompletion() {
        }

        public void afterCompletion(int status) {
            Iterator<Map.Entry<Thread, ThreadTransactionContext>> it = CustomTransactionManager.this.contexts.entrySet().iterator();

            while(it.hasNext()) {
                Map.Entry<Thread, ThreadTransactionContext> entry = it.next();
                ThreadTransactionContext context = entry.getValue();
                if (context.getTransaction() == this.currentTx) {
                    if (log.isDebugEnabled()) {
                        log.debug("clearing thread context: " + context);
                    }

                    it.remove();
                    break;
                }
            }
        }

        public String toString() {
            return "a ClearContextSynchronization for " + this.currentTx;
        }
    }
}
