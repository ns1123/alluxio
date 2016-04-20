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
import alluxio.job.AbstractVoidJobDefinition;
import alluxio.job.JobMasterContext;
import alluxio.job.JobWorkerContext;
import alluxio.job.util.JobUtils;
import alluxio.wire.WorkerInfo;

import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Random;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A job that persists a file into the under storage.
 */
@NotThreadSafe
public final class PersistDefinition extends AbstractVoidJobDefinition<PersistConfig, Void> {
  private static final Logger LOG = LoggerFactory.getLogger(alluxio.Constants.LOGGER_TYPE);

  @Override
  public Map<WorkerInfo, Void> selectExecutors(PersistConfig config,
      List<WorkerInfo> workerInfoList, JobMasterContext jobMasterContext) throws Exception {
    if (workerInfoList.isEmpty()) {
      throw new RuntimeException("No worker is available");
    }

    AlluxioURI uri = new AlluxioURI(config.getFilePath());
    WorkerInfo workerWithMostBlocks = JobUtils.getWorkerWithMostBlocks(workerInfoList,
        jobMasterContext.getFileSystemMaster().getFileBlockInfoList(uri));
    if (workerWithMostBlocks == null) {
      workerWithMostBlocks = workerInfoList.get(new Random().nextInt(workerInfoList.size()));
    }

    Map<WorkerInfo, Void> result = Maps.newHashMap();
    result.put(workerWithMostBlocks, null);
    return result;
  }

  @Override
  public Void runTask(PersistConfig config, Void args, JobWorkerContext jobWorkerContext)
      throws Exception {
    AlluxioURI uri = new AlluxioURI(config.getFilePath());
    FileSystem fileSystem = jobWorkerContext.getFileSystem();
    URIStatus status = fileSystem.getStatus(uri);

    // delete the file if it exists
    if (status.isPersisted()) {
      if (config.isOverwrite()) {
        LOG.info(config.getFilePath() + " is already persisted. Removing it");
        fileSystem.delete(uri);
      } else {
        throw new RuntimeException(
            "File " + config.getFilePath() + " is already persisted, to overwrite the file,"
                + " please set the overwrite flag in the config");
      }
    }

    // persist the file
    long size = FileSystemUtils.persistFile(FileSystem.Factory.get(), uri, status,
        jobWorkerContext.getConfiguration());
    LOG.info("Persisted file " + config.getFilePath() + " with size " + size);
    return null;
  }

}
