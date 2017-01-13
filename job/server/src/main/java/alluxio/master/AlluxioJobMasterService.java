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
import alluxio.PropertyKey;
import alluxio.Server;
import alluxio.master.job.JobMaster;

import java.net.InetSocketAddress;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A job master in the Alluxio system.
 */
public interface AlluxioJobMasterService extends Server {
  /**
   * Factory for creating {@link AlluxioJobMasterService}.
   */
  @ThreadSafe
  final class Factory {
    /**
     * @return a new instance of {@link AlluxioJobMasterService}
     */
    public static AlluxioJobMasterService create() {
      if (Configuration.getBoolean(PropertyKey.ZOOKEEPER_ENABLED)) {
        return new FaultTolerantAlluxioJobMaster();
      }
      return new DefaultAlluxioJobMaster();
    }

    private Factory() {} // prevent instantiation
  }

  /**
   * @return internal {@link JobMaster}
   */
  JobMaster getJobMaster();

  /**
   * @return this master's rpc address
   */
  InetSocketAddress getRpcAddress();

  /**
   * @return the start time of the master in milliseconds
   */
  long getStartTimeMs();

  /**
   * @return the uptime of the master in milliseconds
   */
  long getUptimeMs();

  /**
   * @return the master's web address, or null if the web server hasn't been started yet
   */
  InetSocketAddress getWebAddress();

  /**
   * @return true if the system is the leader (serving the rpc server), false otherwise
   */
  boolean isServing();

  /**
   * Waits until the master is ready to server requests.
   */
  void waitForReady();
}
