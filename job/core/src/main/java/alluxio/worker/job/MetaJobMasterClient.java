/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.worker.job;

import alluxio.exception.ConnectionFailedException;
import alluxio.job.wire.JobMasterInfo;
import alluxio.job.wire.JobMasterInfo.JobMasterInfoField;

import java.io.Closeable;
import java.util.Set;

/**
 * Interface for a client to the job master meta service.
 */
public interface MetaJobMasterClient extends Closeable {
  /**
   * Gets info about the job master.
   *
   * @param jobMasterInfoFields optional list of fields to query; if null all fields will be queried
   * @return the requested master info
   * @throws ConnectionFailedException if the connection fails during the call
   */
  JobMasterInfo getInfo(Set<JobMasterInfoField> jobMasterInfoFields)
      throws ConnectionFailedException;
}
