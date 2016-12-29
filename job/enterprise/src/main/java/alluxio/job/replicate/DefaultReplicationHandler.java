/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.replicate;

import alluxio.AlluxioURI;
import alluxio.client.job.JobRestClientUtils;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The implementation of {@link ReplicationHandler} that utilizes job service.
 */
@ThreadSafe
public final class DefaultReplicationHandler implements ReplicationHandler {
  /**
   * Constructs an instance of {@link DefaultReplicationHandler}.
   */
  public DefaultReplicationHandler() {}

  @Override
  public void evict(AlluxioURI uri, long blockId, int numReplicas) {
    EvictConfig config = new EvictConfig(blockId, numReplicas);
    JobRestClientUtils.runJob(config);
  }

  @Override
  public void replicate(AlluxioURI uri, long blockId, int numReplicas) {
    ReplicateConfig config = new ReplicateConfig(uri.getPath(), blockId, numReplicas);
    JobRestClientUtils.runJob(config);
  }
}
