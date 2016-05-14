/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.load;

import alluxio.Constants;
import alluxio.job.AbstractVoidJobDefinition;
import alluxio.job.JobMasterContext;
import alluxio.job.JobWorkerContext;
import alluxio.wire.WorkerInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A simple loading job that loads the blocks of a file in a distributed and round-robin fashion.
 */
@NotThreadSafe
public final class LoadDefinition extends AbstractVoidJobDefinition<LoadConfig, List<Long>> {
  private static final Logger LOG = LoggerFactory.getLogger(alluxio.Constants.LOGGER_TYPE);
  private static final int BUFFER_SIZE = 500 * Constants.MB;

  @Override
  public Map<WorkerInfo, List<Long>> selectExecutors(LoadConfig config,
      List<WorkerInfo> jobWorkerInfoList, JobMasterContext jobMasterContext) throws Exception {
    throw new UnsupportedOperationException();
  }

  @Override
  public Void runTask(LoadConfig config, List<Long> args, JobWorkerContext jobWorkerContext)
      throws Exception {
    throw new UnsupportedOperationException();
  }
}
