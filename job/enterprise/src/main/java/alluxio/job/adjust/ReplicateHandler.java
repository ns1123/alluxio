/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.adjust;

import alluxio.Constants;
import alluxio.exception.AlluxioException;
import alluxio.job.util.JobRestClientUtils;
import alluxio.master.file.replication.AdjustReplicationHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The implementation to replicate blocks that utilizes job service.
 */
@ThreadSafe
public final class ReplicateHandler implements AdjustReplicationHandler {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /**
   * Constructs an instance of {@link ReplicateHandler}.
   */
  public ReplicateHandler() {}

  @Override
  public void adjust(long blockId, int numReplicas, String path) throws AlluxioException {
    ReplicateConfig config = new ReplicateConfig(blockId, path, numReplicas);
    JobRestClientUtils.runJob(config);
  }
}
