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
import alluxio.PropertyKey;
import alluxio.util.CommonUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * The fault tolerant version of {@link AlluxioJobMaster} that uses Zookeeper and standby masters.
 */
public final class FaultTolerantAlluxioJobMaster extends AlluxioJobMaster {

  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /** The Zookeeper client that handles selecting the leader. */
  private LeaderSelectorClient mLeaderSelectorClient = null;

  public FaultTolerantAlluxioJobMaster() {
    Preconditions.checkArgument(Configuration.getBoolean(PropertyKey.ZOOKEEPER_ENABLED));

    // Set up Zookeeper specific functionality.
    try {
      // InetSocketAddress.toString causes test issues, so build the string by hand
      String zkName = NetworkAddressUtils.getConnectHost(ServiceType.JOB_MASTER_RPC) + ":"
          + getMasterAddress().getPort();
      String zkAddress = Configuration.get(PropertyKey.ZOOKEEPER_ADDRESS);
      String zkElectionPath = Configuration.get(PropertyKey.ZOOKEEPER_JOB_ELECTION_PATH);
      String zkLeaderPath = Configuration.get(PropertyKey.ZOOKEEPER_JOB_LEADER_PATH);
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
      throw Throwables.propagate(e);
    }

    Thread currentThread = Thread.currentThread();
    mLeaderSelectorClient.setCurrentMasterThread(currentThread);
    boolean started = false;

    while (!Thread.interrupted()) {
      if (mLeaderSelectorClient.isLeader()) {
        stopServing();
        stopMasters();

        startMasters(true);
        started = true;
        startServing("(gained leadership)", "(lost leadership)");
      } else {
        // This master should be standby, and not the leader
        if (isServing() || !started) {
          // Need to transition this master to standby mode.
          stopServing();
          stopMasters();

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
