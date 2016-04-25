/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job;

import alluxio.Configuration;
import alluxio.client.file.BaseFileSystem;
import alluxio.client.file.FileSystem;
import alluxio.worker.WorkerContext;
import alluxio.worker.block.BlockWorker;

import com.google.common.base.Preconditions;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The context of worker-side resources and information.
 */
@ThreadSafe
public final class JobWorkerContext {
  private final FileSystem mFileSystem;
  private final BlockWorker mBlockWorker;
  private final Configuration mConfiguration;
  private final long mJobId;
  private final int mTaskId;

  /**
   * @param blockWorker the block worker
   * @param jobId the job id
   * @param taskId the task id
   */
  public JobWorkerContext(BlockWorker blockWorker, long jobId, int taskId) {
    mFileSystem = BaseFileSystem.get();
    mBlockWorker = Preconditions.checkNotNull(blockWorker);
    mConfiguration = WorkerContext.getConf();
    mJobId = jobId;
    mTaskId = taskId;
  }

  /**
   * @return the file system client
   */
  public FileSystem getFileSystem() {
    return mFileSystem;
  }

  /**
   * @return the block worker
   */
  public BlockWorker getBlockWorker() {
    return mBlockWorker;
  }

  /**
   * @return the configuration
   */
  public Configuration getConfiguration() {
    return mConfiguration;
  }

  /**
   * @return the job id
   */
  public long getJobId() {
    return mJobId;
  }

  /**
   * @return the task id
   */
  public int getTaskId() {
    return mTaskId;
  }
}
