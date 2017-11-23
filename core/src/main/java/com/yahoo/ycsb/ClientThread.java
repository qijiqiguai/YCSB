package com.yahoo.ycsb;

import com.yahoo.ycsb.measurements.Measurements;
import com.yahoo.ycsb.target.DB;
import com.yahoo.ycsb.target.DBException;
import com.yahoo.ycsb.workloads.Workload;
import com.yahoo.ycsb.workloads.WorkloadException;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.LockSupport;

/**
 * A thread for executing transactions or data inserts to the database.
 */
public class ClientThread implements Runnable {
  /**
   * Counts down each of the clients completing.
   */
  private final CountDownLatch completeLatch;

    private static boolean spinSleep;
    private DB db;
    private boolean doTransactions;
    private Workload workload;
    private int opCount;
    private double targetOpsPerMs;

    private int opsDone;
    private int threadId;
    private int threadCount;
    private Object workLoadState;
    private Properties props;
    private long targetOpsTickNs;
    private final Measurements measurements;

    /**
     * Constructor.
     *
     * @param db                   the DB implementation to use
     * @param doTransactions       true to do transactions, false to insert data
     * @param workload             the workload to use
     * @param props                the properties defining the experiment
     * @param opCount              the number of operations (transactions or inserts) to do
     * @param targetPerThreadPerms target number of operations per thread per ms
     * @param completeLatch        The latch tracking the completion of all clients.
     */
    public ClientThread(DB db, boolean doTransactions, Workload workload, Properties props, int opCount,
                        double targetPerThreadPerms, CountDownLatch completeLatch) {
      this.db = db;
      this.doTransactions = doTransactions;
      this.workload = workload;
      this.opCount = opCount;
      opsDone = 0;
      if (targetPerThreadPerms > 0) {
        targetOpsPerMs = targetPerThreadPerms;
        targetOpsTickNs = (long) (1000000 / targetOpsPerMs);
      }
      this.props = props;
      measurements = Measurements.getMeasurements();
      spinSleep = Boolean.valueOf(this.props.getProperty("spin.sleep", "false"));
      this.completeLatch = completeLatch;
    }

    public void setThreadId(final int threadId) {
      this.threadId = threadId;
    }

    public void setThreadCount(final int threadCount) {
      this.threadCount = threadCount;
    }

    public int getOpsDone() {
      return opsDone;
    }

    @Override
    public void run() {
      try {
        db.init();
      } catch (DBException e) {
        e.printStackTrace();
        e.printStackTrace(System.out);
        return;
      }

      try {
        workLoadState = workload.initThread(props, threadId, threadCount);
      } catch (WorkloadException e) {
        e.printStackTrace();
        e.printStackTrace(System.out);
        return;
      }

      //NOTE: Switching to using nanoTime and parkNanos for time management here such that the measurements
      // and the client thread have the same view on time.

      //spread the thread operations out so they don't all hit the DB at the same time
      // GH issue 4 - throws exception if _target>1 because random.nextInt argument must be >0
      // and the sleep() doesn't make sense for granularities < 1 ms anyway
      if ((targetOpsPerMs > 0) && (targetOpsPerMs <= 1.0)) {
        long randomMinorDelay = Utils.random().nextInt((int) targetOpsTickNs);
        sleepUntil(System.nanoTime() + randomMinorDelay);
      }
      try {
        if (doTransactions) {
          long startTimeNanos = System.nanoTime();

          while (((opCount == 0) || (opsDone < opCount)) && !workload.isStopRequested()) {

            if (!workload.doTransaction(db, workLoadState)) {
              break;
            }

            opsDone++;

            throttleNanos(startTimeNanos);
          }
        } else {
          long startTimeNanos = System.nanoTime();

          while (((opCount == 0) || (opsDone < opCount)) && !workload.isStopRequested()) {

            if (!workload.doInsert(db, workLoadState)) {
              break;
            }

            opsDone++;

            throttleNanos(startTimeNanos);
          }
        }
      } catch (Exception e) {
        e.printStackTrace();
        e.printStackTrace(System.out);
        System.exit(0);
      }

      try {
        measurements.setIntendedStartTimeNs(0);
        db.cleanup();
      } catch (DBException e) {
        e.printStackTrace();
        e.printStackTrace(System.out);
      } finally {
        completeLatch.countDown();
      }
    }

    private static void sleepUntil(long deadline) {
      while (System.nanoTime() < deadline) {
        if (!spinSleep) {
          LockSupport.parkNanos(deadline - System.nanoTime());
        }
      }
    }

    private void throttleNanos(long startTimeNanos) {
      //throttle the operations
      if (targetOpsPerMs > 0) {
        // delay until next tick
        long deadline = startTimeNanos + opsDone * targetOpsTickNs;
        sleepUntil(deadline);
        measurements.setIntendedStartTimeNs(deadline);
      }
    }

    /**
     * The total amount of work this thread is still expected to do.
     */
    public int getOpsTodo() {
      int todo = opCount - opsDone;
      return todo < 0 ? 0 : todo;
    }
}
