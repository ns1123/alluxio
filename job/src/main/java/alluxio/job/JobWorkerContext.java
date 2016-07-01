/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job;

import alluxio.client.file.BaseFileSystem;
import alluxio.client.file.FileSystem;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The context of worker-side resources and information.
 */
@ThreadSafe
public final class JobWorkerContext {
  private final FileSystem mFileSystem;
  private final long mJobId;
  private final int mTaskId;

  /**
   * Creates a new instance of {@link JobWorkerContext}.
   *
   * @param jobId the job id
   * @param taskId the task id
   */
  public JobWorkerContext(long jobId, int taskId) {
    mFileSystem = BaseFileSystem.get();
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
