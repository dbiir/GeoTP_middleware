package org.apache.shardingsphere.infra.transactions;

public enum XATransactionState {
    NO_TRANSACTION,

    ACTIVE,

    IDLE,

    PREPARED,

    COMMITTED,

    FAILED,

    ABORTED,

    ROLLBACK_ONLY,

    NUM_OF_STATES
}
