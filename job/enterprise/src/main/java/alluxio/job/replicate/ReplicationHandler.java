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

/**
 * Interface for adjusting the replication level of blocks.
 */
public interface ReplicationHandler {

  /**
   * Decreases the block replication level by a target number of replicas.
   *
   * @param uri URI of the file the block belongs to
   * @param blockId ID of the block
   * @param numReplicas how many replicas to add or remove
   */
  void evict(AlluxioURI uri, long blockId, int numReplicas);

  /**
   * Increases the block replication level by a target number of replicas.
   *
   * @param uri URI of the file the block belongs to
   * @param blockId ID of the block
   * @param numReplicas how many replicas to add or remove
   */
  void replicate(AlluxioURI uri, long blockId, int numReplicas);
}
