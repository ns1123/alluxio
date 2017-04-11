/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.client.job;

import alluxio.MasterClient;
import alluxio.exception.AlluxioException;
import alluxio.job.JobConfig;
import alluxio.job.wire.JobInfo;

import java.util.List;

/**
 * Interface for job service clients to communicate with the job master.
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
   * Cancels the given job.
   *
   * @param id the job id
   * @throws AlluxioException if an Alluxio error occurs
   */
  void cancel(long id) throws AlluxioException;

  /**
   * Gets the status of the given job.
   *
   * @param id the job id
   * @return the job information
   * @throws AlluxioException if an Alluxio error occurs
   */
  JobInfo getStatus(long id) throws AlluxioException;

  /**
   * @return the list of ids of all jobs
   * @throws AlluxioException if an Alluxio error occurs
   */
  List<Long> list() throws AlluxioException;

  /**
   * Starts a job based on the given configuration.
   *
   * @param jobConfig the job configuration
   * @return the job id
   * @throws AlluxioException if an Alluxio error occurs
   */
  long run(JobConfig jobConfig) throws AlluxioException;
}
