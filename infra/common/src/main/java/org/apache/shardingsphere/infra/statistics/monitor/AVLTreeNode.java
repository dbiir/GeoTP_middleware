package org.apache.shardingsphere.infra.statistics.monitor;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class AVLTreeNode {
    private int key;
    private int height;
    private LockMetaData lockMetaData;
    AVLTreeNode left, right;

    public AVLTreeNode(int d, LockMetaData lock) {
        this.lockMetaData = lock;
        key = d;
        height = 1;
    }

    public AVLTreeNode(LockMetaData lock) {
        this.lockMetaData = lock;
        key = lock.getKey();
        height = 1;
    }

    public void setLockMetaData(LockMetaData lock) {
        this.lockMetaData = lock;
        this.key = lock.getKey();
    }
}
