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
import alluxio.job.JobWorkerContext;
import alluxio.job.benchmark.compatibility.Operation;

import org.apache.commons.lang3.Validate;

/**
 * Operation involving renaming files.
 */
public final class RenameOperation implements Operation {
  private final FileSystem mFs;
  private final AlluxioURI mRenameUri = new AlluxioURI("/rename");

  private final AlluxioURI mNestedDst1 = mRenameUri.join("d_dst1");
  private final AlluxioURI mNestedDst2 = mNestedDst1.join("d_dst2");
  private final AlluxioURI mNestedDstFile = mNestedDst2.join("f");
  private final AlluxioURI mNestedSrc1 = mRenameUri.join("d_src1");
  private final AlluxioURI mNestedSrc2 = mNestedSrc1.join("d_src2");

  private final AlluxioURI mFileSrc1 = mRenameUri.join("f_src1");
  private final AlluxioURI mFileDst1 = mRenameUri.join("f_dst1");
  private final AlluxioURI mFileSrc2 = mRenameUri.join("f_src2");
  private final AlluxioURI mFileDst2 = mNestedDst2.join("f_dst2");
  private final AlluxioURI mFileNestedSrc = mNestedSrc2.join("f_nested_src");
  private final AlluxioURI mFileNestedDst = mNestedDst2.join("f_nested_dst");
  private final AlluxioURI mDirNestedDst = mNestedDst1.join("d_nested_dst");

  /**
   * Creates a new {@link RenameOperation}.
   *
   * @param context the {@link JobWorkerContext} to use
   */
  public RenameOperation(JobWorkerContext context) {
    mFs = BaseFileSystem.get(FileSystemContext.INSTANCE);
  }

  @Override
  public void generate() throws Exception {
    mFs.createFile(mNestedDstFile).close();
    mFs.createFile(mFileNestedSrc).close();
    mFs.createFile(mFileSrc1).close();
    mFs.createFile(mFileSrc2).close();

    // RenameEntry
    mFs.rename(mFileSrc1, mFileDst1);
    mFs.rename(mFileSrc2, mFileDst2);
    mFs.rename(mFileNestedSrc, mFileNestedDst);
    mFs.rename(mNestedSrc1, mDirNestedDst);
  }

  @Override
  public void validate() throws Exception {
    Validate.isTrue(!mFs.exists(mFileSrc1));
    Validate.isTrue(!mFs.exists(mFileSrc2));
    Validate.isTrue(!mFs.exists(mFileNestedSrc));
    Validate.isTrue(!mFs.exists(mNestedSrc1));

    Validate.isTrue(mFs.exists(mFileDst1));
    Validate.isTrue(mFs.exists(mFileDst2));
    Validate.isTrue(mFs.exists(mFileNestedDst));
    Validate.isTrue(mFs.exists(mDirNestedDst));
  }
}
