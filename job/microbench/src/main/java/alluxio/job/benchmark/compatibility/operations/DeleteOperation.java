/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.benchmark.compatibility.operations;

import alluxio.AlluxioURI;
import alluxio.client.file.BaseFileSystem;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.options.DeleteOptions;
import alluxio.job.JobWorkerContext;
import alluxio.job.benchmark.compatibility.Operation;

import org.apache.commons.lang3.Validate;

/**
 * Operation involving deleting inodes.
 */
public final class DeleteOperation implements Operation {
  private final FileSystem mFs;
  private final AlluxioURI mDeleteUri = new AlluxioURI("/delete");
  private final AlluxioURI mFileDelete = mDeleteUri.join("f_delete");
  private final AlluxioURI mFileNested1 = mDeleteUri.join("d_delete1");
  private final AlluxioURI mFileNested2 = mFileNested1.join("d_delete2");
  private final AlluxioURI mFileNestedDeleted = mFileNested2.join("f_delete");

  /**
   * Creates a new {@link DeleteOperation}.
   *
   * @param context the {@link JobWorkerContext} to use
   */
  public DeleteOperation(JobWorkerContext context) {
    mFs = BaseFileSystem.get(FileSystemContext.INSTANCE);
  }

  @Override
  public void generate() throws Exception {
    mFs.createFile(mFileDelete).close();
    mFs.createFile(mFileNestedDeleted).close();

    // DeleteFileEntry
    mFs.delete(mFileDelete);
    mFs.delete(mFileNested1, DeleteOptions.defaults().setRecursive(true));
  }

  @Override
  public void validate() throws Exception {
    Validate.isTrue(!mFs.exists(mFileDelete));
    Validate.isTrue(!mFs.exists(mFileNested1));
    Validate.isTrue(!mFs.exists(mFileNested2));
    Validate.isTrue(!mFs.exists(mFileNestedDeleted));
  }
}
