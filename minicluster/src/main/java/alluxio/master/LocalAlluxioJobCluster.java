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
import alluxio.PropertyKey;
import alluxio.exception.ConnectionFailedException;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.worker.AlluxioJobWorkerService;

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
  private static final Logger LOG = LoggerFactory.getLogger(LocalAlluxioJobCluster.class);

  private AlluxioJobMasterService mMaster;
  private AlluxioJobWorkerService mWorker;

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
    mMaster.waitForReady();
    startWorker();
    mWorker.waitForReady();
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
  public AlluxioJobMasterService getMaster() {
    return mMaster;
  }

  /**
   * @return the job worker
   */
  public AlluxioJobWorkerService getWorker() {
    return mWorker;
  }

  /**
   * @return the hostname of the cluster
   */
  public String getHostname() {
    return mHostname;
  }

  /**
   * Stops the current worker and starts a new one.
   *
   * @throws Exception if the the worker fails to stop or start
   */
  public void restartWorker() throws Exception {
    mWorker.stop();
    startWorker();
  }

  /**
   * Sets up corresponding directories for tests.
   *
   * @throws IOException when creating or deleting dirs failed
   */
  private void setupTest() throws IOException {}

  /**
   * Updates the test configuration.
   *
   * @throws IOException when the operation fails
   */
  private void updateTestConf() throws IOException {
    setHostname();

    Configuration.set(PropertyKey.JOB_MASTER_BIND_HOST, mHostname);
    Configuration.set(PropertyKey.JOB_MASTER_HOSTNAME, mHostname);
    Configuration.set(PropertyKey.JOB_MASTER_RPC_PORT, Integer.toString(0));
    Configuration.set(PropertyKey.JOB_MASTER_WEB_PORT, Integer.toString(0));
    Configuration.set(PropertyKey.JOB_MASTER_WEB_BIND_HOST, mHostname);
    Configuration.set(PropertyKey.JOB_WORKER_BIND_HOST, mHostname);
    Configuration.set(PropertyKey.JOB_WORKER_RPC_PORT, Integer.toString(0));
    Configuration.set(PropertyKey.JOB_WORKER_WEB_PORT, Integer.toString(0));
    Configuration.set(PropertyKey.JOB_WORKER_WEB_BIND_HOST, mHostname);
  }

  /**
   * Runs a master.
   *
   * @throws IOException if an I/O error occurs
   * @throws ConnectionFailedException if network connection failed
   */
  private void startMaster() throws IOException, ConnectionFailedException {
    mMaster = AlluxioJobMasterService.Factory.create();
    Configuration
        .set(PropertyKey.JOB_MASTER_RPC_PORT, String.valueOf(mMaster.getRpcAddress().getPort()));
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
    mWorker = AlluxioJobWorkerService.Factory.create();
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
