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
import alluxio.exception.AlluxioException;

import java.io.IOException;

/**
 * Interface for adjusting the replication level of blocks.
 */
public interface ReplicationHandler {

  /**
   * Decreases the block replication level by a target number of replicas.
   *
   * @param uri URI of the file the block belongs to
   * @param blockId ID of the block
   * @param numReplicas how many replicas to remove
   * @return the ID of the replicate job
   * @throws AlluxioException if an Alluxio error is encountered
   * @throws IOException if a non-Alluxio error is encountered
   */
  long evict(AlluxioURI uri, long blockId, int numReplicas) throws AlluxioException, IOException;

  /**
   * Increases the block replication level by a target number of replicas.
   *
   * @param uri URI of the file the block belongs to
   * @param blockId ID of the block
   * @param numReplicas how many replicas to add
   * @return the ID of the replicate job
   * @throws AlluxioException if an Alluxio error is encountered
   * @throws IOException if a non-Alluxio error is encountered
   */
  long replicate(AlluxioURI uri, long blockId, int numReplicas)
      throws AlluxioException, IOException;
}
