package org.apache.shardingsphere.infra.transactions;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;

@Slf4j
public class AgentAsyncXAManager {
    static AgentAsyncXAManager Instance = new AgentAsyncXAManager();

    @Getter
    @Setter
    private class XATransactionInfo {
        XATransactionState state;
        String errorInfo;

        public XATransactionInfo(XATransactionState state) {
            this.state = state;
        }
    }

    public static AgentAsyncXAManager getInstance() {
        return Instance;
    }

    private final HashMap<CustomXID, XATransactionInfo> XAStates = new HashMap<>();
    private long globalTid = 0;

    public synchronized long fetchAndAddGlobalTid() {
        return globalTid++;
    }

    public void setStateByXid(CustomXID xid, XATransactionState state) {
        if (XAStates.containsKey(xid)) {
            assert(XAStates.get(xid).getState() == XATransactionState.ACTIVE);
            if (state == XATransactionState.IDLE) {
                // one phase commit
                XAStates.get(xid).setState(state);
            } else if (state == XATransactionState.FAILED) {
                // prepare failed
                XAStates.get(xid).setState(state);
                log.info("prepare failed" + xid);
            } else if (state == XATransactionState.PREPARED) {
                // prepared
                XAStates.get(xid).setState(state);
            } else {
                assert state == XATransactionState.ROLLBACK_ONLY;
                log.info("rollback only" + xid);
            }
        } else {
            assert state == XATransactionState.ACTIVE;
            XATransactionInfo info = new XATransactionInfo(state);
            XAStates.put(xid, info);
        }
    }

    public void setErrorInfoByXid(CustomXID xid, String errorInfo) {
        assert XAStates.containsKey(xid);
        XAStates.get(xid).setErrorInfo(errorInfo);
    }

    public XATransactionState getStateByXid(CustomXID xid) {
        if (XAStates.get(xid) == null) {
            System.out.println(xid.toString());
        }
        return XAStates.get(xid).getState();
    }
}
