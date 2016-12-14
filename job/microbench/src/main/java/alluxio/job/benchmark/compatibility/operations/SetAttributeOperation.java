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
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.SetAttributeOptions;
import alluxio.job.JobWorkerContext;
import alluxio.job.benchmark.compatibility.Operation;

import org.apache.commons.lang3.Validate;

/**
 * Operation involving setting inode attributes.
 */
public final class SetAttributeOperation implements Operation {
  private static final long TTL = 123456789L;
  private final FileSystem mFs;
  private final AlluxioURI mAttrUri = new AlluxioURI("/attr");
  private final AlluxioURI mFileTtl = mAttrUri.join("f_ttl");
  private final AlluxioURI mFilePinned = mAttrUri.join("f_pinned");
  private final AlluxioURI mFilePersisted = mAttrUri.join("f_persisted");

  /**
   * Creates a new {@link SetAttributeOperation}.
   *
   * @param context the {@link JobWorkerContext} to use
   */
  public SetAttributeOperation(JobWorkerContext context) {
    mFs = BaseFileSystem.get(FileSystemContext.INSTANCE);
  }

  @Override
  public void generate() throws Exception {
    mFs.createFile(mFileTtl).close();
    mFs.createFile(mFilePinned).close();
    mFs.createFile(mFilePersisted).close();

    // SetAttributeEntry
    mFs.setAttribute(mFileTtl, SetAttributeOptions.defaults().setTtl(TTL));
    mFs.setAttribute(mFilePinned, SetAttributeOptions.defaults().setPinned(true));
    mFs.setAttribute(mFilePersisted, SetAttributeOptions.defaults().setPersisted(true));
  }

  @Override
  public void validate() throws Exception {
    URIStatus status;

    status = mFs.getStatus(mFileTtl);
    Validate.isTrue(status.getTtl() == TTL);
    status = mFs.getStatus(mFilePinned);
    Validate.isTrue(status.isPinned());
    status = mFs.getStatus(mFilePersisted);
    Validate.isTrue(status.getPersistenceState().equals("PERSISTED"));
  }
}
