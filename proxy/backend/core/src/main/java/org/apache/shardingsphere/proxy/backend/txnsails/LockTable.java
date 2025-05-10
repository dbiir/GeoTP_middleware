package org.apache.shardingsphere.proxy.backend.txnsails;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LockTable {
  private static final LockTable INSTANCE;
  private static final int SMALL_BANK_HASH_SIZE = 4000000;
  private static final int YCSB_HASH_SIZE = 100000;
  public static int LOAD_THREAD = 16;
  private int HASH_SIZE;
  private HashMap<String, LinkedList<ValidationLock>[]> validationLocks = new HashMap<>(4);
  private HashMap<String, ReentrantReadWriteLock[]> validationBucketLocks = new HashMap<>(4);
  private final long lockWaitTimeout = 10;
  private final int maxRetry = 5;
  private String workload;
  public final String GetSavingsBalance =
          "SELECT vid FROM savings WHERE custid = ?";
  public final String GetCheckingBalance =
          "SELECT vid FROM checking WHERE custid = ?";
  public final String GetUserTable =
          "SELECT vid FROM usertable WHERE  ycsb_key = ?";

  static {
    INSTANCE = new LockTable();
  }

  public LockTable() {

  }

  public String generateFetchSQL(final PreValidationInfo info) {
    if (workload.equals("smallbank")) {
      if (info.getTable().equalsIgnoreCase("savings")) {
        return GetSavingsBalance;
      } else if (info.getTable().equalsIgnoreCase("checking")) {
        return GetCheckingBalance;
      }
    } else if (workload.equals("ycsb")) {
      return GetUserTable;
    } else {
      System.out.println("unknown workload: " + workload);
      return "";
    }
    return "";
  }

  public void initHotspot(String workload, List<Connection> connections) throws SQLException {
    this.workload = workload;
    if (workload.equals("smallbank")) {
      this.HASH_SIZE = SMALL_BANK_HASH_SIZE;
      this.LOAD_THREAD = 1;
      // init validation locks
      validationLocks.put("savings", new LinkedList[HASH_SIZE]);
      validationLocks.put("checking", new LinkedList[HASH_SIZE]);
      validationBucketLocks.put("savings", new ReentrantReadWriteLock[HASH_SIZE]);
      validationBucketLocks.put("checking", new ReentrantReadWriteLock[HASH_SIZE]);
      for (int i = 0; i < HASH_SIZE; i++) {
        validationLocks.get("savings")[i] = new LinkedList<>();
        validationLocks.get("checking")[i] = new LinkedList<>();
        validationLocks.get("savings")[i].add(new ValidationLock(i));
        validationLocks.get("checking")[i].add(new ValidationLock(i));
        validationBucketLocks.get("savings")[i] = new ReentrantReadWriteLock();
        validationBucketLocks.get("checking")[i] = new ReentrantReadWriteLock();
      }

      ExecutorService executor = Executors.newFixedThreadPool(LOAD_THREAD);

      for (int i = 0; i < LOAD_THREAD; i++) {
        int index = i;

        executor.submit(() -> {
          PreparedStatement getSavings = null;
          PreparedStatement getChecking = null;
          try {
            getSavings = connections.get(index).prepareStatement(GetSavingsBalance);
            getChecking = connections.get(index).prepareStatement(GetCheckingBalance);
          } catch (SQLException e) {
            throw new RuntimeException(e);
          }

          for (int j = index; j < HASH_SIZE; j += LOAD_THREAD) {
            try {
              if (!validationLocks.get("savings")[j].isEmpty()) {
                getSavings.setLong(1, index);
                try (ResultSet res = getSavings.executeQuery()) {
                  if (!res.next()) {
                    String msg = "Invalid custid #%d" + j;
                    throw new RuntimeException(msg);
                  }
                  updateHotspotVersion("savings", j, res.getLong(1));
                }
              }

              if (!validationLocks.get("checking")[j].isEmpty()) {
                getChecking.setLong(1, j);
                try (ResultSet res = getChecking.executeQuery()) {
                  if (!res.next()) {
                    String msg = "Invalid custid #" + j;
                    throw new RuntimeException(msg);
                  }
                  updateHotspotVersion("checking", j, res.getLong(1));
                }
              }
            } catch (SQLException ex) {
              throw new RuntimeException(ex);
            }
          }
        });
      }

      executor.shutdown();
    } else if (workload.equals("ycsb")) {
      System.out.println("init ycsb lock table");
      this.HASH_SIZE = YCSB_HASH_SIZE;
      // init validation locks
      validationLocks.put("usertable", new LinkedList[HASH_SIZE]);
      validationBucketLocks.put("usertable", new ReentrantReadWriteLock[HASH_SIZE]);
      for (int i = 0; i < HASH_SIZE; i++) {
        validationLocks.get("usertable")[i] = new LinkedList<>();
        validationLocks.get("usertable")[i].add(new ValidationLock(i));
        validationBucketLocks.get("usertable")[i] = new ReentrantReadWriteLock();
      }
      ExecutorService executor = Executors.newFixedThreadPool(LOAD_THREAD);

      for (int i = 0; i < LOAD_THREAD; i++) {
        int index = i;

        executor.submit(() -> {
          PreparedStatement getUserTable = null;
          try {
            getUserTable = connections.get(index).prepareStatement(GetUserTable);
            for (int j = index; j < HASH_SIZE; j += LOAD_THREAD) {
              if (!validationLocks.get("usertable")[j].isEmpty()) {
                getUserTable.setInt(1, j);
                try (ResultSet rs = getUserTable.executeQuery()) {
                  if (!rs.next()) {
                    String msg = "Invalid ycsb key: #" + j;
                    throw new RuntimeException(msg);
                  }
                  updateHotspotVersion("usertable", j, rs.getLong(1));
                }
              }
            }
          } catch (SQLException e) {
            throw new RuntimeException(e);
          }
        });
      }
      executor.shutdown();
    } else if (workload.equals("tpcc")) {
      throw new SQLException("not implemented", "501");
    }

    for (int i = 0; i < LOAD_THREAD; i++) {
      connections.get(i).close();
    }
  }

  public void tryValidationLock(String table, long tid, long key, LockType type) throws SQLException {
    if (!validationLocks.containsKey(table)) {
      String msg = "unknown table name: " + table;
      throw new RuntimeException(msg);
    }

    if (checkAndTryValidationLock(table, tid, key, type))
      return;
    try {
      tryAndAddValidationLock(table, tid, key, type);
    } catch (SQLException ex){
      throw ex;
    }
  }

  public void releaseValidationLock(String table, long key, LockType type) {
    if (!validationLocks.containsKey(table)) {
      String msg = "unknown table name: " + table;
      throw new RuntimeException(msg);
    }
    int bucketNum = (int)(key % HASH_SIZE);
    validationBucketLocks.get(table)[bucketNum].readLock().lock();
    List<ValidationLock> lockList = validationLocks.get(table)[bucketNum];
    for (ValidationLock lock: lockList) {
      if (lock.getId() == key) {
        lock.releaseLock(type);
        break;
      }
    }
    validationBucketLocks.get(table)[bucketNum].readLock().unlock();

    // shrink the validation lock list
    if (validationBucketLocks.get(table)[bucketNum].writeLock().tryLock()) {
      lockList.removeIf(lock -> lock.getId() > HASH_SIZE && lock.free());
      validationBucketLocks.get(table)[bucketNum].writeLock().unlock();
    }
  }

  /**
   * @return: true if you get validation lock, otherwise can not find the validation lock
   */
  private boolean checkAndTryValidationLock(String table, long tid, long key, LockType type) throws SQLException {
    int bucketNum = (int)(key % HASH_SIZE);
    long currentTime = System.currentTimeMillis();
    validationBucketLocks.get(table)[bucketNum].readLock().lock();
    List<ValidationLock> lockList = validationLocks.get(table)[bucketNum];
    for (ValidationLock lock: lockList) {
      if (lock.getId() == key) {
        int res = 0;
        int count = 0;
        while((res = lock.tryLock(tid, type)) == 0)  {
          try {
            Thread.sleep(0, 100);
            // System.out.println(Thread.currentThread().getName() + " wait for lock " + key + " " + table + ", lock type is " + type);
          } catch (InterruptedException ex) {
            System.out.println("out of the max retry count, lock type is " + type);
            res = -1;
            break;
          }
          count++;
        }
        if (res > 0) {
          validationBucketLocks.get(table)[bucketNum].readLock().unlock();
          return true;
        } else {
          // can not keep the sequence of read and write
          String msg = "Transaction #" + tid + " can not keep the sequence of rw dependency, there maybe rw-anti-dependency";
          validationBucketLocks.get(table)[bucketNum].readLock().unlock();
          throw new SQLException(msg, "500");
        }
      }
    }
    validationBucketLocks.get(table)[bucketNum].readLock().unlock();
    return false;
  }

  public void tryAndAddValidationLock(String table, long tid, long key, LockType type) throws SQLException {
    int bucketNum = (int)(key % HASH_SIZE);
    validationBucketLocks.get(table)[bucketNum].writeLock().lock();
    List<ValidationLock> lockList = validationLocks.get(table)[bucketNum];
    for (ValidationLock lock: lockList) {
      if (lock.getId() == key) {
        int res;
        int count = 0;
        while((res = lock.tryLock(tid, type)) == 0)  {
          try {
            Thread.sleep(0, 10000);
          } catch (InterruptedException ex) {
            System.out.println("out of the max retry count, lock type is " + type);
            res = -1;
            break;
          }
          count++;
        }
        validationBucketLocks.get(table)[bucketNum].writeLock().unlock();
        if (res <= 0) {
          // can not keep the sequence of read and write
          String msg = "can not keep the sequence of rw dependency, there maybe rw-anti-dependency";
          throw new SQLException(msg, "500");
        }
        return;
      }
    }
    ValidationLock newLock = new ValidationLock(key);
    validationLocks.get(table)[bucketNum].add(newLock);
    newLock.tryLock(tid, type);
    validationBucketLocks.get(table)[bucketNum].writeLock().unlock();
  }

  public void updateHotspotVersion(String table, long key, long vid) {
    int bucketNum = (int)(key % HASH_SIZE);
    validationBucketLocks.get(table)[bucketNum].readLock().lock();
    for (ValidationLock lock: validationLocks.get(table)[bucketNum]) {
      if (lock.getId() == key) {
        lock.updateVersion(vid);
        break;
      }
    }
    validationBucketLocks.get(table)[bucketNum].readLock().unlock();
  }

  public long getHotspotVersion(String table, long key) {
    if (!validationLocks.containsKey(table)) {
      String msg = "unknown table name: " + table;
      throw new RuntimeException(msg);
    }
    int bucketNum = (int)(key % HASH_SIZE);
    validationBucketLocks.get(table)[bucketNum].readLock().lock();
    for (ValidationLock lock: validationLocks.get(table)[bucketNum]) {
      if (lock.getId() == key) {
        validationBucketLocks.get(table)[bucketNum].readLock().unlock();
        return lock.getVersion();
      }
    }
    validationBucketLocks.get(table)[bucketNum].readLock().unlock();
    return -1;
  }

  public static LockTable getInstance() {
    return INSTANCE;
  }
}
