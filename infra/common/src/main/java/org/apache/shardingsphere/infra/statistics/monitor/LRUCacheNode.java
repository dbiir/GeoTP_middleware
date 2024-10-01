package org.apache.shardingsphere.infra.statistics.monitor;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class LRUCacheNode {
    private int key;
    private LockMetaData lockMetaData;
    private LRUCacheNode prev;
    private LRUCacheNode next;

    public LRUCacheNode(int key, LockMetaData lockMetaData) {
        this.key = key;
        this.lockMetaData = lockMetaData;
    }

    public LRUCacheNode(LockMetaData lockMetaData) {
        this.key = lockMetaData.getKey();
        this.lockMetaData = lockMetaData;
    }

    public boolean canBeRemoved() {
        return lockMetaData.getProcessing() == 0;
    }
}
