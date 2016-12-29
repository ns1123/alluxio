/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.client.job;

import alluxio.exception.AlluxioException;
import alluxio.job.wire.JobInfo;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

/**
 * Interface for job service clients to communicate with the job master.
 */
public interface JobMasterClient {
  /**
   * Cancels the given job.
   *
   * @param id the job id
   * @throws AlluxioException if an Alluxio error occurs
   * @throws IOException if an IO error occurs
   */
  void cancel(long id) throws AlluxioException, IOException;

  /**
   * Gets the status of the given job.
   *
   * @param id the job id
   * @return the job information
   * @throws AlluxioException if an Alluxio error occurs
   * @throws IOException if an IO error occurs
   */
  JobInfo getStatus(long id) throws AlluxioException, IOException;

  /**
   * @return the list of ids of all jobs
   * @throws AlluxioException if an Alluxio error occurs
   * @throws IOException if an IO error occurs
   */
  List<Long> list() throws AlluxioException, IOException;

  /**
   * Starts a job based on the given configuration.
   *
   * @param jobConfig the command line job info
   * @return the job id
   * @throws AlluxioException if an Alluxio error occurs
   * @throws IOException if an IO error occurs
   */
  long run(Serializable jobConfig) throws AlluxioException, IOException;
}
