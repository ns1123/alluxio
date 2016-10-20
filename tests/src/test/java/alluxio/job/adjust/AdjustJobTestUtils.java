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

import alluxio.client.block.BlockMasterClient;
import alluxio.client.block.BlockStoreContext;
import alluxio.client.block.BlockWorkerClient;
import alluxio.resource.CloseableResource;
import alluxio.wire.BlockInfo;
import alluxio.wire.WorkerNetAddress;

import java.io.IOException;

/**
 * Utils for adjust replication job integration tests.
 */
public final class AdjustJobTestUtils {

  private AdjustJobTestUtils() {
  } // prevent instantiation

  /**
   * Queries block master for the given block.
   *
   * @param blockId block ID
   * @param context handler of BlockStoreContext instance
   * @return the BlockInfo of this block
   * @throws Exception if any error happens
   */
  public static BlockInfo getBlockInfoFromMaster(long blockId, BlockStoreContext context)
      throws Exception {
    try (CloseableResource<BlockMasterClient> blockMasterClientResource = context
        .acquireMasterClientResource()) {
      return blockMasterClientResource.get().getBlockInfo(blockId);
    }
  }

  /**
   * Checks whether a block is still on the worker.
   *
   * @param blockId block ID
   * @param address worker address
   * @param context handler of BlockStoreContext instance
   * @return the BlockInfo of this block
   * @throws Exception if any error happens
   */
  public static boolean checkBlockOnWorker(long blockId, WorkerNetAddress address,
      BlockStoreContext context) throws Exception {
    try (BlockWorkerClient blockWorkerClient = context.createWorkerClient(address)) {
      boolean found = true;
      try {
        blockWorkerClient.accessBlock(blockId);
      } catch (Exception e) {
        found = false;
      }
      return found;
    }
  }
}
