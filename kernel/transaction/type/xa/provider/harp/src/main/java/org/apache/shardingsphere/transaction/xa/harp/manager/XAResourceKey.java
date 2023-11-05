package org.apache.shardingsphere.transaction.xa.harp.manager;

import lombok.Getter;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;

public class XAResourceKey {
    @Getter
    private XAResource xares;

    public XAResourceKey(XAResource xares) {
        this.xares = xares;
    }

    public boolean equals(Object o) {
        boolean ret = false;
        if (o instanceof XAResourceKey) {
            XAResourceKey other = (XAResourceKey)o;

            try {
                ret = other.xares == this.xares || other.xares.isSameRM(this.xares);
            } catch (XAException var5) {
            }
        }

        return ret;
    }

    public int hashCode() {
        return this.xares.getClass().getName().toString().hashCode();
    }

    public String toString() {
        return this.xares.toString();
    }
}
