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
 * The context is used by job to access master-side resources.
 */
@ThreadSafe
public final class JobMasterContext {
  private final FileSystem mFileSystem;

  /**
   * Creates a new instance of {@link JobMasterContext}.
   */
  public JobMasterContext() {
    mFileSystem = BaseFileSystem.get();
  }

  /**
   * @return the file system client
   */
  public FileSystem getFileSystem() {
    return mFileSystem;
  }
}
