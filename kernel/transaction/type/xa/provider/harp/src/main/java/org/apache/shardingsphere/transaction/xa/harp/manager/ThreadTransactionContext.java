package org.apache.shardingsphere.transaction.xa.harp.manager;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.shardingsphere.infra.transactions.XATransactionState;

@Setter
@Slf4j
public class ThreadTransactionContext {
    private volatile CustomTransactionImp transaction;
    private volatile int timeout = 0;
    @Getter
    XATransactionState state;

    public ThreadTransactionContext() {
        state = XATransactionState.NO_TRANSACTION;
        transaction = null;
    }

    public CustomTransactionImp getTransaction() {
        return this.transaction;
    }

    public void setTransaction(CustomTransactionImp transaction) {
        if (transaction == null) {
            throw new IllegalArgumentException("transaction parameter cannot be null");
        } else {
            if (log.isDebugEnabled()) {
                log.debug("assigning <" + transaction + "> to <" + this + ">");
            }

            this.transaction = transaction;
        }
    }

    public int getTimeout() {
        return this.timeout;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    public String toString() {
        return "a ThreadContext with transaction " + this.transaction;
    }
}
