/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.shell.command;

import alluxio.AlluxioURI;
import alluxio.client.FileSystemTestUtils;
import alluxio.client.WriteType;
import alluxio.client.file.URIStatus;
import alluxio.exception.AlluxioException;
import alluxio.shell.AbstractAlluxioShellTest;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * Test for {@link EnterpriseLoadCommand}.
 */
public final class EnterpriseLoadCommandTest extends AbstractAlluxioShellTest {
  @Test
  public void loadDir() throws IOException, AlluxioException {
    FileSystemTestUtils.createByteFile(mFileSystem, "/testRoot/testFileA", WriteType.THROUGH, 10);
    FileSystemTestUtils
        .createByteFile(mFileSystem, "/testRoot/testFileB", WriteType.MUST_CACHE, 10);
    AlluxioURI uriA = new AlluxioURI("/testRoot/testFileA");
    AlluxioURI uriB = new AlluxioURI("/testRoot/testFileB");

    URIStatus statusA = mFileSystem.getStatus(uriA);
    URIStatus statusB = mFileSystem.getStatus(uriB);
    Assert.assertFalse(statusA.getInMemoryPercentage() == 100);
    Assert.assertTrue(statusB.getInMemoryPercentage() == 100);
    // Testing loading of a directory
    mFsShell.run("enterprise-load", "/testRoot");
    statusA = mFileSystem.getStatus(uriA);
    statusB = mFileSystem.getStatus(uriB);
    Assert.assertTrue(statusA.getInMemoryPercentage() == 100);
    Assert.assertTrue(statusB.getInMemoryPercentage() == 100);
  }

  @Test
  public void loadFile() throws IOException, AlluxioException {
    FileSystemTestUtils.createByteFile(mFileSystem, "/testFile", WriteType.THROUGH, 10);
    AlluxioURI uri = new AlluxioURI("/testFile");
    URIStatus status = mFileSystem.getStatus(uri);
    Assert.assertFalse(status.getInMemoryPercentage() == 100);
    // Testing loading of a single file
    mFsShell.run("enterprise-load", "/testFile");
    status = mFileSystem.getStatus(uri);
    Assert.assertTrue(status.getInMemoryPercentage() == 100);
  }
}
