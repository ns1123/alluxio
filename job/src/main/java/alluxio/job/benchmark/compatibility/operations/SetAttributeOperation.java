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
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.SetAttributeOptions;
import alluxio.job.benchmark.compatibility.Operation;

import org.apache.commons.lang3.Validate;

/**
 * Operation involving setting inode attributes.
 */
public final class SetAttributeOperation implements Operation {
  private final FileSystem mFs;
  private final AlluxioURI mAttrUri = new AlluxioURI("/attr");

  public SetAttributeOperation(FileSystem fs) {
    mFs = fs;
  }

  @Override
  public void generate() throws Exception {
    mFs.createFile(mAttrUri.join("f_ttl")).close();
    mFs.createFile(mAttrUri.join("f_pinned")).close();
    mFs.createFile(mAttrUri.join("f_persisted")).close();

    // SetAttributeEntry
    mFs.setAttribute(mAttrUri.join("f_ttl"), SetAttributeOptions.defaults().setTtl(123456789L));
    mFs.setAttribute(mAttrUri.join("f_pinned"), SetAttributeOptions.defaults().setPinned(true));
    mFs.setAttribute(mAttrUri.join("f_persisted"), SetAttributeOptions.defaults().setPersisted(true));
  }

  @Override
  public void validate() throws Exception {
    URIStatus status;

    status = mFs.getStatus(mAttrUri.join("f_ttl"));
    Validate.isTrue(status.getTtl() == 123456789L);
    status = mFs.getStatus(mAttrUri.join("f_pinned"));
    Validate.isTrue(status.isPinned());
    status = mFs.getStatus(mAttrUri.join("f_persisted"));
    Validate.isTrue(status.getPersistenceState().equals("PERSISTED"));
  }
}
