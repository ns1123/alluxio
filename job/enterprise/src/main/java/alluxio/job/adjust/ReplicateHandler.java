/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.job.adjust;

import alluxio.Constants;
import alluxio.exception.AlluxioException;
import alluxio.job.util.JobRestClientUtils;
import alluxio.master.file.replication.AdjustReplicationHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The implementation to replicate blocks that utilizes job service.
 */
public final class ReplicateHandler implements AdjustReplicationHandler {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /**
   * Constructs an instance of {@link ReplicateHandler}.
   */
  public ReplicateHandler() {}

  @Override
  public void adjust(long blockId, int numReplicas) throws AlluxioException {
    ReplicateConfig config = new ReplicateConfig(blockId, numReplicas);
    JobRestClientUtils.runJob(config);
  }
}
