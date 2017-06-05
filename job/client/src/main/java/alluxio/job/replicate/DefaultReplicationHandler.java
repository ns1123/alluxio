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
import alluxio.client.job.JobMasterClient;
import alluxio.client.job.JobMasterClientPool;
import alluxio.exception.AlluxioException;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The implementation of {@link ReplicationHandler} that utilizes job service.
 */
@ThreadSafe
public final class DefaultReplicationHandler implements ReplicationHandler {
  private final JobMasterClientPool mJobMasterClientPool;

  /**
   * Creates a new instance of {@link DefaultReplicationHandler}.
   *
   * @param jobMasterClientPool job master client pool
   */
  public DefaultReplicationHandler(JobMasterClientPool jobMasterClientPool) {
    mJobMasterClientPool = jobMasterClientPool;
  }

  @Override
  public long evict(AlluxioURI uri, long blockId, int numReplicas)
      throws AlluxioException, IOException {
    JobMasterClient client = mJobMasterClientPool.acquire();
    try {
      return client.run(new EvictConfig(blockId, numReplicas));
    } finally {
      mJobMasterClientPool.release(client);
    }
  }

  @Override
  public long replicate(AlluxioURI uri, long blockId, int numReplicas)
      throws AlluxioException, IOException {
    JobMasterClient client = mJobMasterClientPool.acquire();
    try {
      return client.run(new ReplicateConfig(uri.getPath(), blockId, numReplicas));
    } finally {
      mJobMasterClientPool.release(client);
    }
  }
}
