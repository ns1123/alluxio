/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.worker.job;

import alluxio.MasterClient;
import alluxio.exception.AlluxioException;
import alluxio.exception.ConnectionFailedException;
import alluxio.thrift.JobCommand;
import alluxio.thrift.TaskInfo;
import alluxio.wire.WorkerNetAddress;

import java.io.IOException;
import java.util.List;

/**
 * Interface for job service workers to communicate with the job master.
 */
public interface JobMasterClient extends MasterClient {

  /**
   * Factory for {@link JobMasterClient}.
   */
  class Factory {

    private Factory() {} // prevent instantiation

    /**
     * Factory method for {@link JobMasterClient}.
     *
     * @return a new {@link JobMasterClient} instance
     */
    public static JobMasterClient create() {
      return RetryHandlingJobMasterClient.create();
    }
  }

  /**
   * Returns a worker id for a workers net address.
   *
   * @param address the net address to get a worker id for
   * @return a worker id
   * @throws ConnectionFailedException if network connection failed
   * @throws IOException if an I/O error occurs
   */
  long registerWorker(final WorkerNetAddress address) throws IOException, ConnectionFailedException;

  /**
   * Periodic heartbeats to update the tasks' status from a worker, and returns the commands.
   *
   * @param workerId the worker id
   * @param taskInfoList the list of the task information
   * @return the commands issued to the worker
   * @throws AlluxioException if an Alluxio error occurs
   * @throws IOException if an I/O error occurs
   */
  List<JobCommand> heartbeat(final long workerId, final List<TaskInfo> taskInfoList)
      throws AlluxioException, IOException;
}
