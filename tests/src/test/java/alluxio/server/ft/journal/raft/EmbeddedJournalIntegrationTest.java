/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.server.ft.journal.raft;

import static org.junit.Assert.assertTrue;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.client.file.FileSystem;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.multi.process.MultiProcessCluster;
import alluxio.multi.process.MultiProcessCluster.DeployMode;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;

import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Integration tests for the embedded journal.
 */
public final class EmbeddedJournalIntegrationTest extends BaseIntegrationTest {
  private static final int NUM_MASTERS = 3;

  @Rule
  public MultiProcessCluster mCluster = MultiProcessCluster.newBuilder()
      .setClusterName("EmbeddedJournalIntegrationTest")
      .setNumMasters(NUM_MASTERS)
      .setNumWorkers(0)
      .setDeployMode(DeployMode.EMBEDDED_HA)
      .addProperty(PropertyKey.MASTER_JOURNAL_FLUSH_TIMEOUT_MS, "5min")
      .addProperty(PropertyKey.MASTER_JOURNAL_TAILER_SHUTDOWN_QUIET_WAIT_TIME_MS, "50ms")
      .build();

  @Test
  public void failover() throws Exception {
    AlluxioURI testDir = new AlluxioURI("/dir");
    FileSystem fs = mCluster.getFileSystemClient();
    fs.createDirectory(testDir);
    mCluster.waitForAndKillPrimaryMaster(30 * Constants.SECOND_MS);
    assertTrue(fs.exists(testDir));
    mCluster.notifySuccess();
  }

  @Test
  public void restart() throws Exception {
    AlluxioURI testDir = new AlluxioURI("/dir");
    FileSystem fs = mCluster.getFileSystemClient();
    fs.createDirectory(testDir);
    restartMasters();
    assertTrue(fs.exists(testDir));
    restartMasters();
    assertTrue(fs.exists(testDir));
    restartMasters();
    assertTrue(fs.exists(testDir));
    mCluster.saveWorkdir();
    mCluster.notifySuccess();
  }

  @Test
  public void restartStress() throws Throwable {
    // Run and verify operations while restarting the cluster multiple times.
    AtomicReference<Throwable> failure = new AtomicReference<>();
    AtomicInteger successes = new AtomicInteger(0);
    FileSystem fs = mCluster.getFileSystemClient();
    List<OperationThread> threads = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      OperationThread t = new OperationThread(fs, i, failure, successes);
      t.start();
      threads.add(t);
    }
    for (int i = 0; i < 3; i++) {
      System.out.printf("---------- Iteration %s ----------\n", i);
      successes.set(0);
      CommonUtils.waitFor("25 successes", x -> successes.get() >= 25,
          WaitForOptions.defaults().setTimeoutMs(30 * Constants.SECOND_MS));
      if (failure.get() != null) {
        throw failure.get();
      }
      restartMasters();
    }
  }

  private void restartMasters() throws Exception {
    for (int i = 0; i < NUM_MASTERS; i++) {
      mCluster.stopMaster(i);
    }
    for (int i = 0; i < NUM_MASTERS; i++) {
      mCluster.startMaster(i);
    }
  }

  private static class OperationThread extends Thread {
    private final FileSystem mFs;
    private final int mThreadNum;
    private final AtomicReference<Throwable> mFailure;
    private final AtomicInteger mSuccessCounter;

    public OperationThread(FileSystem fs, int threadNum, AtomicReference<Throwable> failure,
        AtomicInteger successCounter) {
      super("operation-test-thread-" + threadNum);
      mFs = fs;
      mThreadNum = threadNum;
      mFailure = failure;
      mSuccessCounter = successCounter;
    }

    public void run() {
      try {
        runInternal();
      } catch (Exception e) {
        e.printStackTrace();
        mFailure.set(e);
      }
    }

    public void runInternal() throws Exception {
      while (!Thread.interrupted()) {
        for (int i = 0; i < 100; i++) {
          AlluxioURI dir = formatDirName(i);
          try {
            mFs.createDirectory(dir);
          } catch (FileAlreadyExistsException e) {
            // This could happen if the operation was retried but actually succeeded server-side on
            // the first attempt. Alluxio does not de-duplicate retried operations.
            continue;
          }
          if (!mFs.exists(dir)) {
            mFailure.set(new RuntimeException(String.format("Directory %s does not exist", dir)));
            return;
          }
        }
        for (int i = 0; i < 100; i++) {
          AlluxioURI dir = formatDirName(i);
          try {
            mFs.delete(dir);
          } catch (FileDoesNotExistException e) {
            // This could happen if the operation was retried but actually succeeded server-side on
            // the first attempt. Alluxio does not de-duplicate retried operations.
            continue;
          }
          if (mFs.exists(dir)) {
            mFailure.set(new RuntimeException(String.format("Directory %s still exists", dir)));
            return;
          }
        }
        System.out.println("Success counter: " + mSuccessCounter.incrementAndGet());
      }
    }

    private AlluxioURI formatDirName(int dirNum) {
      return new AlluxioURI(String.format("/dir-%d-%d", mThreadNum, dirNum));
    }
  }
}
