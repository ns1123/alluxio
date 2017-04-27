/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.worker;

import alluxio.Process;
import alluxio.wire.WorkerNetAddress;

import java.net.InetSocketAddress;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A job worker in the Alluxio system.
 */
public interface JobWorkerProcess extends Process {
  /**
   * Factory for creating {@link JobWorkerProcess}.
   */
  @ThreadSafe
  final class Factory {
    /**
     * @return a new instance of {@link JobWorkerProcess}
     */
    public static JobWorkerProcess create() {
      return new AlluxioJobWorkerProcess();
    }

    private Factory() {} // prevent instantiation
  }

  /**
   * @return the connect information for this worker
   */
  WorkerNetAddress getAddress();

  /**
   * @return this worker's rpc address
   */
  InetSocketAddress getRpcAddress();

  /**
   * @return the start time of the worker in milliseconds
   */
  long getStartTimeMs();

  /**
   * @return the uptime of the worker in milliseconds
   */
  long getUptimeMs();

  /**
   * @return the master's web address, or null if the web server hasn't been started yet
   */
  InetSocketAddress getWebAddress();
}
