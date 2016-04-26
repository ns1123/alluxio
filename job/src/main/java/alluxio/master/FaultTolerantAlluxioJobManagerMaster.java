/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.master;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.LeaderSelectorClient;
import alluxio.master.job.JobManagerMaster;
import alluxio.master.journal.ReadOnlyJournal;
import alluxio.util.CommonUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * The fault tolerant version of {@link AlluxioJobManagerMaster} that uses zookeeper and standby
 * masters.
 */
@NotThreadSafe
final class FaultTolerantAlluxioJobManagerMaster extends AlluxioJobManagerMaster {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /** The zookeeper client that handles selecting the leader. */
  private LeaderSelectorClient mLeaderSelectorClient = null;

  public FaultTolerantAlluxioJobManagerMaster() {
    super();
    Configuration conf = MasterContext.getConf();
    Preconditions.checkArgument(conf.getBoolean(Constants.ZOOKEEPER_ENABLED));

    // Set up zookeeper specific functionality.
    try {
      // InetSocketAddress.toString causes test issues, so build the string by hand
      String zkName =
          NetworkAddressUtils.getConnectHost(ServiceType.JOB_MANAGER_MASTER_RPC, conf) + ":"
              + getMasterAddress().getPort();
      String zkAddress = conf.get(Constants.ZOOKEEPER_ADDRESS);
      String zkElectionPath = conf.get(Constants.ZOOKEEPER_ELECTION_PATH);
      String zkLeaderPath = conf.get(Constants.ZOOKEEPER_LEADER_PATH);
      mLeaderSelectorClient =
          new LeaderSelectorClient(zkAddress, zkElectionPath, zkLeaderPath, zkName);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void start() throws Exception {
    try {
      mLeaderSelectorClient.start();
    } catch (IOException e) {
      LOG.error(e.getMessage(), e);
      throw Throwables.propagate(e);
    }

    Thread currentThread = Thread.currentThread();
    mLeaderSelectorClient.setCurrentMasterThread(currentThread);
    boolean started = false;

    while (true) {
      if (mLeaderSelectorClient.isLeader()) {
        stopServing();
        stopMasters();

        // Transitioning from standby to master, replace readonly journal with writable journal.
        mJobManagerMaster.upgradeToReadWriteJournal(mJobMasterJournal);

        startMasters(true);
        started = true;
        startServing("(gained leadership)", "(lost leadership)");
      } else {
        // This master should be standby, and not the leader
        if (isServing() || !started) {
          // Need to transition this master to standby mode.
          stopServing();
          stopMasters();

          // When transitioning from master to standby, recreate the masters with a readonly
          // journal.
          mJobManagerMaster =
              new JobManagerMaster(new ReadOnlyJournal(mJobMasterJournal.getDirectory()));
          startMasters(false);
          started = true;
        }
        // This master is already in standby mode. No further actions needed.
      }

      CommonUtils.sleepMs(LOG, 100);
    }
  }

  @Override
  public void stop() throws Exception {
    super.stop();
    if (mLeaderSelectorClient != null) {
      mLeaderSelectorClient.close();
    }
  }
}
