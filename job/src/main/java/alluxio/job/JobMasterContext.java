/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job;

import alluxio.master.block.BlockMaster;
import alluxio.master.file.FileSystemMaster;

import com.google.common.base.Preconditions;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The context is used by job to access master-side resources and information.
 */
@ThreadSafe
public final class JobMasterContext {
  private final FileSystemMaster mFileSystemMaster;
  private final BlockMaster mBlockMaster;
  private final long mJobId;

  /**
   * @param fileSystemMaster the file system master
   * @param blockMaster the block master
   * @param jobId the job id
   */
  public JobMasterContext(FileSystemMaster fileSystemMaster, BlockMaster blockMaster, long jobId) {
    mFileSystemMaster = Preconditions.checkNotNull(fileSystemMaster);
    mBlockMaster = Preconditions.checkNotNull(blockMaster);
    mJobId = jobId;
  }

  /**
   * @return the file system master
   */
  public FileSystemMaster getFileSystemMaster() {
    return mFileSystemMaster;
  }

  /**
   * @return the block master
   */
  public BlockMaster getBlockMaster() {
    return mBlockMaster;
  }

  /**
   * @return the job id
   */
  public long getJobId() {
    return mJobId;
  }
}
