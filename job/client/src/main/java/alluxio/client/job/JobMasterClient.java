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
import alluxio.job.JobConfig;
import alluxio.job.wire.JobInfo;

import java.io.IOException;
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
   */
  void cancel(long id) throws IOException;

  /**
   * Gets the status of the given job.
   *
   * @param id the job id
   * @return the job information
   */
  JobInfo getStatus(long id) throws IOException;

  /**
   * @return the list of ids of all jobs
   */
  List<Long> list() throws IOException;

  /**
   * Starts a job based on the given configuration.
   *
   * @param jobConfig the job configuration
   * @return the job id
   */
  long run(JobConfig jobConfig) throws IOException;
}
