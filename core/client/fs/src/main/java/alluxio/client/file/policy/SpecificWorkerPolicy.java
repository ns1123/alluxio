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

package alluxio.client.file.policy;

import alluxio.client.block.BlockWorkerInfo;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.status.UnavailableException;
import alluxio.wire.WorkerNetAddress;

import com.google.common.base.Preconditions;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A policy which only returns the worker that is specified by the config.
 */
@ThreadSafe
public final class SpecificWorkerPolicy implements FileWriteLocationPolicy {
  private final WorkerNetAddress mWorkerAddress;

  /**
   * Constructs the policy with the specific worker address.
   *
   * @param workerAddress the address of the worker
   */
  public SpecificWorkerPolicy(WorkerNetAddress workerAddress) {
    mWorkerAddress = Preconditions.checkNotNull(workerAddress);
  }

  @Override
  public WorkerNetAddress getWorkerForNextBlock(Iterable<BlockWorkerInfo> workerInfoList,
      long blockSizeBytes) throws UnavailableException {
    for (BlockWorkerInfo info : workerInfoList) {
      if (info.getNetAddress().equals(mWorkerAddress)) {
        return info.getNetAddress();
      }
    }
    throw new UnavailableException(
        ExceptionMessage.NO_SPACE_FOR_BLOCK_ON_WORKER.getMessage(blockSizeBytes));
  }
}
