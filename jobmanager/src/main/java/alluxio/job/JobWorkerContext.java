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

import javax.annotation.concurrent.ThreadSafe;

/**
 * The context of worker-side resources.
 */
@ThreadSafe
public final class JobWorkerContext {
  private final FileSystem mFileSystem;
  private final Configuration mConfiguration;

  /**
   * Creates a new instance of {@link JobWorkerContext}.
   */
  public JobWorkerContext() {
    mFileSystem = BaseFileSystem.get();
    mConfiguration = WorkerContext.getConf();
  }

  /**
   * @return the file system client
   */
  public FileSystem getFileSystem() {
    return mFileSystem;
  }

  /**
   * @return the configuration
   */
  public Configuration getConfiguration() {
    return mConfiguration;
  }
}
