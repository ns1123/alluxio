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
 * Operation involving mounting and unmounting.
 */
public final class MountOperation implements Operation {
  private final FileSystem mFs;
  private final AlluxioURI mMntUri = new AlluxioURI("/mnt");
  private final String mHome;

  public MountOperation(FileSystem fs, String home) {
    mFs = fs;
    mHome = home;
  }

  @Override
  public void generate() throws Exception {
    mFs.createDirectory(mMntUri);

    // AddMountPointEntry
    mFs.mount(mMntUri.join("mnt"), new AlluxioURI(mHome + "/assembly"));
    mFs.mount(mMntUri.join("mnt_delete"), new AlluxioURI(mHome + "/bin"));

    // DeleteMountPointEntry
    mFs.unmount(mMntUri.join("mnt_delete"));
  }

  @Override
  public void validate() throws Exception {
    Validate.isTrue(mFs.exists(mMntUri.join("mnt")));
    Validate.isTrue(!mFs.exists(mMntUri.join("mnt_delete")));
  }
}
