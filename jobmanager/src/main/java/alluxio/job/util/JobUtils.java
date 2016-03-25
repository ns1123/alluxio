/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.util;

import alluxio.collections.IndexedSet;
import alluxio.collections.IndexedSet.FieldIndex;
import alluxio.wire.BlockLocation;
import alluxio.wire.FileBlockInfo;
import alluxio.wire.WorkerInfo;

import com.google.common.collect.Maps;

import java.util.List;
import java.util.concurrent.ConcurrentMap;

/**
 * Utility class to make it easier to write jobs.
 */
public final class JobUtils {
  private static final FieldIndex<WorkerInfo> WORKER_ADDRESS_INDEX = new FieldIndex<WorkerInfo>() {
    @Override
    public Object getFieldValue(WorkerInfo o) {
      return o.getAddress();
    }
  };

  /**
   * Returns whichever specified worker stores the most blocks from the block info list.
   *
   * @param workers a list of workers to consider
   * @param fileBlockInfos a list of file block information
   * @return a worker address storing the most blocks from the list
   */
  public static WorkerInfo getWorkerWithMostBlocks(List<WorkerInfo> workers,
      List<FileBlockInfo> fileBlockInfos) {
    // Index workers by their addresses.
    IndexedSet<WorkerInfo> addressIndexedWorkers = new IndexedSet<WorkerInfo>(WORKER_ADDRESS_INDEX);
    addressIndexedWorkers.addAll(workers);

    // Use ConcurrentMap for putIfAbsent. A regular Map works in Java 8.
    ConcurrentMap<WorkerInfo, Integer> blocksPerWorker = Maps.newConcurrentMap();
    int maxBlocks = 0;
    WorkerInfo mostBlocksWorker = null;
    for (FileBlockInfo fileBlockInfo : fileBlockInfos) {
      for (BlockLocation location : fileBlockInfo.getBlockInfo().getLocations()) {
        WorkerInfo worker = addressIndexedWorkers.getFirstByField(WORKER_ADDRESS_INDEX,
            location.getWorkerAddress());
        if (worker == null) {
          // We can only choose workers in the workers list.
          continue;
        }
        blocksPerWorker.putIfAbsent(worker, 0);
        int newBlockCount = blocksPerWorker.get(worker) + 1;
        blocksPerWorker.put(worker, newBlockCount);
        if (newBlockCount > maxBlocks) {
          maxBlocks = newBlockCount;
          mostBlocksWorker = worker;
        }
      }
    }
    return mostBlocksWorker;
  }

  private JobUtils() {} // Utils class not intended for instantiation.
}
