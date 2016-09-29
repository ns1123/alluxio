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
import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.file.BaseFileSystem;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.FileSystemUtils;
import alluxio.client.file.URIStatus;
import alluxio.job.AbstractVoidJobDefinition;
import alluxio.job.JobMasterContext;
import alluxio.job.JobWorkerContext;
import alluxio.job.util.JobUtils;
import alluxio.job.util.SerializableVoid;
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
public final class PersistDefinition
    extends AbstractVoidJobDefinition<PersistConfig, SerializableVoid> {
  private static final Logger LOG = LoggerFactory.getLogger(alluxio.Constants.LOGGER_TYPE);
  private final FileSystemContext mFileSystemContext;
  private final FileSystem mFileSystem;

  /**
   * Constructs a new {@link PersistDefinition}.
   */
  public PersistDefinition() {
    mFileSystemContext = FileSystemContext.INSTANCE;
    mFileSystem = BaseFileSystem.get(FileSystemContext.INSTANCE);
  }

  /**
   * Constructs a new {@link PersistDefinition} with FileSystem context and instance.
   *
   * @param context file system context
   * @param fileSystem file system client
   */
  public PersistDefinition(FileSystemContext context, FileSystem fileSystem) {
    mFileSystemContext = context;
    mFileSystem = fileSystem;
  }

  @Override
  public Map<WorkerInfo, SerializableVoid> selectExecutors(PersistConfig config,
      List<WorkerInfo> jobWorkerInfoList, JobMasterContext jobMasterContext) throws Exception {
    if (jobWorkerInfoList.isEmpty()) {
      throw new RuntimeException("No worker is available");
    }

    AlluxioURI uri = new AlluxioURI(config.getFilePath());
    List<BlockWorkerInfo> alluxioWorkerInfoList =
        mFileSystemContext.getAlluxioBlockStore().getWorkerInfoList();
    BlockWorkerInfo workerWithMostBlocks = JobUtils.getWorkerWithMostBlocks(alluxioWorkerInfoList,
        mFileSystem.getStatus(uri).getFileBlockInfos());

    // Map the best Alluxio worker to a job worker.
    Map<WorkerInfo, SerializableVoid> result = Maps.newHashMap();
    boolean found = false;
    if (workerWithMostBlocks != null) {
      for (WorkerInfo workerInfo : jobWorkerInfoList) {
        if (workerInfo.getAddress().getHost()
            .equals(workerWithMostBlocks.getNetAddress().getHost())) {
          result.put(workerInfo, null);
          found = true;
          break;
        }
      }
    }
    if (!found) {
      result.put(jobWorkerInfoList.get(new Random().nextInt(jobWorkerInfoList.size())), null);
    }

    return result;
  }

  @Override
  public SerializableVoid runTask(PersistConfig config, SerializableVoid args,
      JobWorkerContext jobWorkerContext) throws Exception {
    AlluxioURI uri = new AlluxioURI(config.getFilePath());

    URIStatus status = mFileSystem.getStatus(uri);

    // delete the file if it exists
    if (status.isPersisted()) {
      if (config.isOverwrite()) {
        LOG.info(config.getFilePath() + " is already persisted. Removing it");
        mFileSystem.delete(uri);
      } else {
        throw new RuntimeException(
            "File " + config.getFilePath() + " is already persisted, to overwrite the file,"
                + " please set the overwrite flag in the config");
      }
    }

    // persist the file
    long size = FileSystemUtils.persistFile(FileSystem.Factory.get(), uri, status);
    LOG.info("Persisted file " + config.getFilePath() + " with size " + size);
    return null;
  }

  @Override
  public Class<PersistConfig> getJobConfigClass() {
    return PersistConfig.class;
  }
}
