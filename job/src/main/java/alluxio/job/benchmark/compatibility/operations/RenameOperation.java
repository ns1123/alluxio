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
import alluxio.client.file.FileSystem;
import alluxio.job.benchmark.compatibility.Operation;

import org.apache.commons.lang3.Validate;

/**
 * Operation involving renaming files
 */
public final class RenameOperation implements Operation {
  private final FileSystem mFs;
  private final AlluxioURI mRenameUri = new AlluxioURI("/rename");

  public RenameOperation(FileSystem fs) {
    mFs = fs;
  }

  @Override
  public void generate() throws Exception {
    mFs.createFile(mRenameUri.join("d_dst1").join("d_dst2").join("f")).close();
    mFs.createFile(mRenameUri.join("d_src1").join("d_src2").join("f_nested_src")).close();
    mFs.createFile(mRenameUri.join("f_src1")).close();
    mFs.createFile(mRenameUri.join("f_src2")).close();

    // RenameEntry
    mFs.rename(mRenameUri.join("f_src1"), mRenameUri.join("f_dst1"));
    mFs.rename(mRenameUri.join("f_src2"), mRenameUri.join("d_dst1").join("d_dst2").join("f_dst2"));
    mFs.rename(mRenameUri.join("d_src1").join("d_src2").join("f_nested_src"),
        mRenameUri.join("d_dst1").join("f_nested_dst"));
    mFs.rename(mRenameUri.join("d_src1"), mRenameUri.join("d_dst1").join("d_dst1"));
  }

  @Override
  public void validate() throws Exception {
    Validate.isTrue(!mFs.exists(mRenameUri.join("f_src1")));
    Validate.isTrue(!mFs.exists(mRenameUri.join("f_src2")));
    Validate.isTrue(!mFs.exists(mRenameUri.join("d_src1").join("d_src2").join("f_nested_src")));
    Validate.isTrue(!mFs.exists(mRenameUri.join("d_src1")));

    Validate.isTrue(mFs.exists(mRenameUri.join("f_dst1")));
    Validate.isTrue(mFs.exists(mRenameUri.join("d_dst1").join("d_dst2").join("f_dst2")));
    Validate.isTrue(mFs.exists(mRenameUri.join("d_dst1").join("f_nested_dst")));
    Validate.isTrue(mFs.exists(mRenameUri.join("d_dst1").join("d_dst1").join("d_src2")));
  }
}
