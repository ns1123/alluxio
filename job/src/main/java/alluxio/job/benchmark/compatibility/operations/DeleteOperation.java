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
import alluxio.client.file.options.DeleteOptions;
import alluxio.job.benchmark.compatibility.Operation;

import org.apache.commons.lang3.Validate;

/**
 * Operation involving deleting inodes.
 */
public final class DeleteOperation implements Operation {
  private final FileSystem mFs;
  private final AlluxioURI mDeleteUri = new AlluxioURI("/delete");

  public DeleteOperation(FileSystem fs) {
    mFs = fs;
  }

  @Override
  public void generate() throws Exception {
    mFs.createFile(mDeleteUri.join("f_delete")).close();
    mFs.createFile(mDeleteUri.join("d_delete1").join("d_delete2").join("f_delete")).close();

    // DeleteFileEntry
    mFs.delete(mDeleteUri.join("f_delete"));
    mFs.delete(mDeleteUri.join("d_delete1"), DeleteOptions.defaults().setRecursive(true));
  }

  @Override
  public void validate() throws Exception {
    Validate.isTrue(!mFs.exists(mDeleteUri.join("f_delete")));
    Validate.isTrue(!mFs.exists(mDeleteUri.join("d_delete1")));
    Validate.isTrue(!mFs.exists(mDeleteUri.join("d_delete1").join("d_delete2")));
    Validate.isTrue(!mFs.exists(mDeleteUri.join("d_delete1").join("d_delete2").join("f_delete")));
  }
}
