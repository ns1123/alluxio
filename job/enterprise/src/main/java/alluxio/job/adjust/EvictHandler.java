/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.adjust;

import alluxio.exception.AlluxioException;
import alluxio.job.util.JobRestClientUtils;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The implementation to evict blocks that utilizes job service.
 */
@ThreadSafe
public final class EvictHandler implements AdjustReplicationHandler {

  /**
   * Constructs an instance of {@link EvictHandler}.
   */
  public EvictHandler() {}

  @Override
  public void adjust(long blockId, int numReplicas) throws AlluxioException {
    EvictConfig config = new EvictConfig(blockId, numReplicas);
    JobRestClientUtils.runJob(config);
  }
}
