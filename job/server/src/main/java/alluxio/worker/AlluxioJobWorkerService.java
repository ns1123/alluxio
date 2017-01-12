/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.worker;

import alluxio.Server;

import java.net.InetSocketAddress;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A job worker in the Alluxio system.
 */
public interface AlluxioJobWorkerService extends Server {
  /**
   * Factory for creating {@link AlluxioJobWorkerService}.
   */
  @ThreadSafe
  final class Factory {
    /**
     * @return a new instance of {@link AlluxioJobWorkerService}
     */
    public static AlluxioJobWorkerService create() {
      return new DefaultAlluxioJobWorker();
    }

    private Factory() {} // prevent instantiation
  }

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

  /**
   * Waits until the worker is ready to server requests.
   */
  void waitForReady();
}
