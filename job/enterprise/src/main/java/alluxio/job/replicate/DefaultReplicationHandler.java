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
import alluxio.client.job.JobThriftClientUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The implementation of {@link ReplicationHandler} that utilizes job service.
 */
@ThreadSafe
public final class DefaultReplicationHandler implements ReplicationHandler {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultReplicationHandler.class);

  /**
   * Creates a new instance of {@link DefaultReplicationHandler}.
   */
  public DefaultReplicationHandler() {}

  @Override
  public void evict(AlluxioURI uri, long blockId, int numReplicas) {
    EvictConfig config = new EvictConfig(blockId, numReplicas);
    try {
      JobThriftClientUtils.start(config);
    } catch (Exception e) {
      LOG.warn("Unexpected exception encountered when starting evict job (uri={}, block id={}, "
          + "delta={})", uri.toString(), blockId, numReplicas, e);
    }
  }

  @Override
  public void replicate(AlluxioURI uri, long blockId, int numReplicas) {
    ReplicateConfig config = new ReplicateConfig(uri.getPath(), blockId, numReplicas);
    try {
      JobThriftClientUtils.start(config);
    } catch (Exception e) {
      LOG.warn("Unexpected exception encountered when starting  replicate job (uri={}, "
          + "block id={}, delta={})", uri.toString(), blockId, numReplicas, e);
    }
  }
}
