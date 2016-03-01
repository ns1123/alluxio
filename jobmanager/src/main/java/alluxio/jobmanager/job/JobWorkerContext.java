/*************************************************************************
* Copyright (c) 2016 Alluxio, Inc.  All rights reserved.
*
* This software and all information contained herein is confidential and
* proprietary to Alluxio, and is protected by copyright and other
* applicable laws in the United States and other jurisdictions.  You may
* not use, modify, reproduce, distribute, or disclose this software
* without the express written permission of Alluxio.
**************************************************************************/

package alluxio.jobmanager.job;

import alluxio.client.file.BaseFileSystem;
import alluxio.client.file.FileSystem;
import alluxio.worker.block.BlockWorker;

import com.google.common.base.Preconditions;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The context of worker-side resources.
 */
@ThreadSafe
public final class JobWorkerContext {
  private final FileSystem mFileSystem;
  private final BlockWorker mBlockWorker;

  /**
   * @param blockWorker the block worker
   */
  public JobWorkerContext(BlockWorker blockWorker) {
    mFileSystem = BaseFileSystem.get();
    mBlockWorker = Preconditions.checkNotNull(blockWorker);
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
}
