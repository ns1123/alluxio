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
import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.client.file.BaseFileSystem;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.job.JobWorkerContext;
import alluxio.job.benchmark.compatibility.Operation;

import org.apache.commons.lang3.Validate;

/**
 * Operation involving mounting and unmounting.
 */
public final class MountOperation implements Operation {
  private final FileSystem mFs;
  private final String mHome;
  private final AlluxioURI mMntUri = new AlluxioURI("/mnt");
  private final AlluxioURI mMntCreate = mMntUri.join("mnt_create");
  private final AlluxioURI mMntDelete = mMntUri.join("mnt_delete");

  /**
   * Creates a new {@link MountOperation}.
   *
   * @param context the {@link JobWorkerContext} to use
   */
  public MountOperation(JobWorkerContext context) {
    mFs = BaseFileSystem.get(FileSystemContext.INSTANCE);
    mHome = Configuration.get(PropertyKey.HOME);
  }

  @Override
  public void generate() throws Exception {
    mFs.createDirectory(mMntUri);

    // AddMountPointEntry
    mFs.mount(mMntCreate, new AlluxioURI(mHome + "/assembly"));
    mFs.mount(mMntDelete, new AlluxioURI(mHome + "/bin"));

    // DeleteMountPointEntry
    mFs.unmount(mMntDelete);
  }

  @Override
  public void validate() throws Exception {
    Validate.isTrue(mFs.exists(mMntCreate));
    Validate.isTrue(!mFs.exists(mMntDelete));
  }
}
