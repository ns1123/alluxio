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

package alluxio.master;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.exception.ConnectionFailedException;
import alluxio.master.job.JobMaster;
import alluxio.master.job.JobMasterPrivateAccess;
import alluxio.util.CommonUtils;
import alluxio.util.UnderFileSystemUtils;
import alluxio.util.io.PathUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.worker.AlluxioJobWorker;
import alluxio.worker.JobWorkerIdRegistry;

import org.powermock.reflect.Whitebox;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Local Alluxio job cluster. This cluster is should only be used in conjunction with
 * an {@link AbstractLocalAlluxioCluster}.
 */
@NotThreadSafe
public final class LocalAlluxioJobCluster {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private static final long CLUSTER_READY_POLL_INTERVAL_MS = 10;
  private static final long CLUSTER_READY_TIMEOUT_MS = 60000;
  private static final String ELLIPSIS = "…";

  private AlluxioJobMaster mMaster;
  private AlluxioJobWorker mWorker;

  private String mHostname;

  private Thread mMasterThread;
  private Thread mWorkerThread;

  /**
   * Creates a new instance of {@link LocalAlluxioJobCluster}.
   */
  public LocalAlluxioJobCluster() {}

  /**
   * Starts both master and a worker using the configurations in test conf respectively.
   *
   * @throws IOException if an I/O error occurs
   * @throws ConnectionFailedException if network connection failed
   */
  public void start() throws IOException, ConnectionFailedException {
    LOG.info("Start Alluxio job service");
    setupTest();
    updateTestConf();
    startMaster();
    waitForMasterReady();
    startWorker();
    waitForWorkerReady();
  }

  /**
   * Stops the alluxio job service threads.
   *
   * @throws Exception when the operation fails
   */
  public void stop() throws Exception {
    LOG.info("Stop Alluxio job service");
    mWorker.stop();
    mMaster.stop();
  }

  /**
   * @return the job master
   */
  public AlluxioJobMaster getMaster() {
    return mMaster;
  }

  /**
   * @return the hostname of the cluster
   */
  public String getHostname() {
    return mHostname;
  }

  /**
   * Waits for the master to be ready.
   *
   * Specifically, waits for it to be possible to connect to the master's rpc and web ports.
   */
  private void waitForMasterReady() {
    long startTime = System.currentTimeMillis();
    String actionMessage = "waiting for master to serve web";
    LOG.info(actionMessage + ELLIPSIS);
    // The port should be set properly after the server has started
    while (!NetworkAddressUtils.isServing(mMaster.getWebBindHost(), mMaster.getWebLocalPort())
        || Configuration.getInt(PropertyKey.JOB_MASTER_WEB_PORT) == 0) {
      waitAndCheckTimeout(startTime, actionMessage);
    }
    actionMessage = "waiting for master to serve rpc";
    LOG.info(actionMessage + ELLIPSIS);
    // The port should be set properly after the server has started
    while (!NetworkAddressUtils.isServing(mMaster.getRPCBindHost(), mMaster.getRPCLocalPort())
        || Configuration.getInt(PropertyKey.JOB_MASTER_RPC_PORT) == 0) {
      waitAndCheckTimeout(startTime, actionMessage);
    }
  }

  /**
   * Waits for the worker to be ready.
   *
   * Specifically, waits for the worker to register with the master and for it to be possible to
   * connect to the worker's data, rpc, and web ports.
   */
  private void waitForWorkerReady() {
    long startTime = System.currentTimeMillis();
    String actionMessage = "waiting for worker to register with master";
    LOG.info(actionMessage + ELLIPSIS);
    while (!workerRegistered()) {
      waitAndCheckTimeout(startTime, actionMessage);
    }
    actionMessage = "waiting for worker to serve rpc";
    LOG.info(actionMessage + ELLIPSIS);
    // The port should be set properly after the server has started
    while (!NetworkAddressUtils.isServing(mWorker.getRPCBindHost(), mWorker.getRPCLocalPort())
        || Configuration.getInt(PropertyKey.JOB_WORKER_RPC_PORT) == 0) {
      waitAndCheckTimeout(startTime, actionMessage);
    }
  }

  /**
   * Checks whether the time since startTime has exceeded the maximum timeout, then sleeps for
   * {@link #CLUSTER_READY_POLL_INTERVAL_MS}ms.
   *
   * @param startTime the time to compare against the current time to check for timeout
   * @param actionMessage a message describing the action being waited for; this message is included
   *        in the error message reported if timeout occurs
   */
  private void waitAndCheckTimeout(long startTime, String actionMessage) {
    if (System.currentTimeMillis() - startTime > CLUSTER_READY_TIMEOUT_MS) {
      throw new RuntimeException("Failed to start cluster. Timed out " + actionMessage);
    }
    CommonUtils.sleepMs(CLUSTER_READY_POLL_INTERVAL_MS);
  }

  /**
   * @return whether the worker has registered with the master
   */
  private boolean workerRegistered() {
    long workerId = JobWorkerIdRegistry.getWorkerId();
    if (workerId == JobWorkerIdRegistry.INVALID_WORKER_ID) {
      return false;
    }
    JobMaster jobMaster = mMaster.getJobMaster();
    return JobMasterPrivateAccess.isWorkerRegistered(jobMaster, workerId);
  }

  /**
   * Sets up corresponding directories for tests.
   *
   * @throws IOException when creating or deleting dirs failed
   */
  private void setupTest() throws IOException {
    // Formats the journal
    String journalFolder = Configuration.get(PropertyKey.MASTER_JOURNAL_FOLDER);
    UnderFileSystemUtils.mkdirIfNotExists(journalFolder);
    for (String masterServiceName : AlluxioJobMaster.getServiceNames()) {
      UnderFileSystemUtils
          .mkdirIfNotExists(PathUtils.concatPath(journalFolder, masterServiceName));
    }
  }

  /**
   * Updates the test configuration.
   *
   * @throws IOException when the operation fails
   */
  private void updateTestConf() throws IOException {
    setHostname();

    Configuration.set(PropertyKey.JOB_MASTER_HOSTNAME, mHostname);
    Configuration.set(PropertyKey.JOB_MASTER_RPC_PORT, Integer.toString(0));
    Configuration.set(PropertyKey.JOB_MASTER_WEB_PORT, Integer.toString(0));
    Configuration.set(PropertyKey.JOB_MASTER_BIND_HOST, mHostname);
    Configuration.set(PropertyKey.JOB_MASTER_WEB_BIND_HOST, mHostname);
    Configuration.set(PropertyKey.JOB_WORKER_RPC_PORT, Integer.toString(0));
    Configuration.set(PropertyKey.JOB_WORKER_BIND_HOST, mHostname);
  }

  /**
   * Runs a master.
   *
   * @throws IOException if an I/O error occurs
   * @throws ConnectionFailedException if network connection failed
   */
  private void startMaster() throws IOException, ConnectionFailedException {
    mMaster = new AlluxioJobMaster();
    Whitebox.setInternalState(AlluxioJobMaster.class, "sAlluxioJobMaster", mMaster);

    Configuration.set(PropertyKey.JOB_MASTER_RPC_PORT, String.valueOf(mMaster.getRPCLocalPort()));

    Runnable runMaster = new Runnable() {
      @Override
      public void run() {
        try {
          mMaster.start();

        } catch (Exception e) {
          throw new RuntimeException(e + " \n Start Master Error \n" + e.getMessage(), e);
        }
      }
    };
    mMasterThread = new Thread(runMaster);
    mMasterThread.start();
  }

  /**
   * Runs a worker.
   *
   * @throws IOException if an I/O error occurs
   * @throws ConnectionFailedException if network connection failed
   */
  private void startWorker() throws IOException, ConnectionFailedException {
    mWorker = new AlluxioJobWorker();
    Whitebox.setInternalState(AlluxioJobWorker.class, "sAlluxioJobWorker", mWorker);

    Runnable runWorker = new Runnable() {
      @Override
      public void run() {
        try {
          mWorker.start();

        } catch (Exception e) {
          throw new RuntimeException(e + " \n Start Worker Error \n" + e.getMessage(), e);
        }
      }
    };
    mWorkerThread = new Thread(runWorker);
    mWorkerThread.start();
  }

  /**
   * Sets hostname.
   */
  private void setHostname() {
    mHostname = NetworkAddressUtils.getLocalHostName(100);
  }
}
