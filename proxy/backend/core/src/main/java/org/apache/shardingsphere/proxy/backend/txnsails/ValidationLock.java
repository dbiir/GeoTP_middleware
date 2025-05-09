package org.apache.shardingsphere.proxy.backend.txnsails;

import lombok.Getter;
import lombok.Setter;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ValidationLock {
  private LockType type; // SH for read validation, EX for write commit check
  private final Lock lock;
  private int count;
  private long maxTid;
  private long minWriteWaitTid;
  private long minReadWaitTid;
  @Getter
  private final long id;
  @Getter
  long version;
  @Getter
  @Setter
  long versionOldRead;
  @Getter
  @Setter
  long versionOldWrite;
  @Getter
  @Setter
  long versionNewRead;
  @Getter
  @Setter
  long versionNewWrite;

  public ValidationLock(long id) {
    this.lock = new ReentrantLock();
    this.type = LockType.NoneType;
    this.count = 0;
    this.maxTid = 0;
    this.minWriteWaitTid = 0;
    this.minReadWaitTid = 0;
    this.id = id;
    this.version = -1;
  }

  synchronized public void updateVersion(long v) {
    version = v > version ? v : version;
  }

  /*
   * @return result. 1 for success, 0 means the transaction manager can wait, -1 for abort
   */
  public int tryLock(long tid, LockType lockType) {
    int result = -2;
    this.lock.lock();
    if (this.type == LockType.NoneType) {
      this.type = lockType;
      assert (this.count == 0);
      count++;
      if (lockType == LockType.EX) {
        minWriteWaitTid = 0;
      } else if (lockType == LockType.SH) {
        minReadWaitTid = 0;
      }
      this.maxTid = tid;
      result = 1;
    } else if (this.type == LockType.SH) {
      // this type is SH
      if (lockType == LockType.SH) {
        if (minWriteWaitTid != 0 && tid > minWriteWaitTid) {
          // an old transaction wants to read the entry, abort
          result = -1;
        } else {
          count++;
          this.maxTid = Math.max(this.maxTid, tid);
          result = 1;
        }
      } else {
        if (minWriteWaitTid != 0) {
          // it wants to acquire a write lock, while there is a concurrent write send commit before it
          minWriteWaitTid = Math.min(minWriteWaitTid, tid);
          result = 0;
        } else {
          minWriteWaitTid = tid;
          result = 0;
        }

        if (maxTid > tid) {
          result = -1;
        }
      }
    } else {
      // this type is EX
      result = -1;
    }
    this.lock.unlock();
//        if (result == 1) {
//            assert (this.type == lockType);
//            System.out.println("acquire id: " + id + " " + this.type + ", count: " + (count));
//        }
    return result;
  }

  public void releaseLock(LockType lockType) {
    this.lock.lock();
//        System.out.println("release id: " + id + " " + this.type + ", " + lockType + " count: " + count);
//        if (this.type != lockType) {
//            System.out.println("failure-release id: " + id + " " + this.type + ", " + lockType + " count: " + count);
//        }
    assert (this.type == lockType);
    count--;
    if (count == 0) {
      this.type = LockType.NoneType;
      this.maxTid = 0;
    }
    this.lock.unlock();
  }

  public boolean free() {
    return this.type == LockType.NoneType;
  }
}
