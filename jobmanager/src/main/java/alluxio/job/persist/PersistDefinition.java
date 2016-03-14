/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.persist;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemUtils;
import alluxio.client.file.URIStatus;
import alluxio.job.JobDefinition;
import alluxio.job.JobMasterContext;
import alluxio.job.JobWorkerContext;
import alluxio.wire.BlockLocation;
import alluxio.wire.FileBlockInfo;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;

import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A job that persists a file into the under storage.
 */
@NotThreadSafe
public final class PersistDefinition implements JobDefinition<PersistConfig, Void> {
  private static final Logger LOG = LoggerFactory.getLogger(alluxio.Constants.LOGGER_TYPE);

  @Override
  public Map<WorkerInfo, Void> selectExecutors(PersistConfig config,
      List<WorkerInfo> workerInfoList, JobMasterContext jobMasterContext) throws Exception {
    if (workerInfoList.isEmpty()) {
      throw new RuntimeException("No worker is available");
    }

    // find the worker that has the most blocks
    AlluxioURI uri = config.getPath();

    Map<WorkerNetAddress, Integer> workerToCount = Maps.newHashMap();
    for (FileBlockInfo fileBlockInfo : jobMasterContext.getFileSystemMaster()
        .getFileBlockInfoList(uri)) {
      if (fileBlockInfo.getBlockInfo().getLocations().isEmpty()) {
        throw new RuntimeException(
            "Block " + fileBlockInfo.getBlockInfo().getBlockId() + " does not exist");
      }
      for (BlockLocation location : fileBlockInfo.getBlockInfo().getLocations()) {
        WorkerNetAddress address = location.getWorkerAddress();
        if (!workerToCount.containsKey(address)) {
          workerToCount.put(address, 1);
        }
        workerToCount.put(address, workerToCount.get(address) + 1);
      }
    }

    WorkerInfo toReturn = workerInfoList.get(0);
    int maxCount = 0;
    for (WorkerInfo workerInfo : workerInfoList) {
      if (!workerToCount.containsKey(workerInfo.getAddress())) {
        // the worker does not have any block
        continue;
      }
      if (workerToCount.get(workerInfo.getAddress()) > maxCount) {
        toReturn = workerInfo;
        maxCount = workerToCount.get(workerInfo.getAddress());
      }
    }

    Map<WorkerInfo, Void> result = Maps.newHashMap();
    result.put(toReturn, null);

    return result;
  }

  @Override
  public void runTask(PersistConfig config, Void args, JobWorkerContext jobWorkerContext)
      throws Exception {
    FileSystem fileSystem = jobWorkerContext.getFileSystem();
    URIStatus status = fileSystem.getStatus(config.getPath());

    // delete the file if it exists
    // TODO(yupeng) the delete is not necessary if the file in the under storage has the same size
    if (status.isPersisted()) {
      LOG.info(config.getPath() + " is already persisted. Removing it");
      fileSystem.delete(config.getPath());
    }

    // persist the file
    long size = FileSystemUtils.persistFile(FileSystem.Factory.get(), config.getPath(), status,
        jobWorkerContext.getConfiguration());
    LOG.info("Persisted file " + config.getPath() + " with size " + size);
  }

}
