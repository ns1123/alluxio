/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.shell.command;

import alluxio.AlluxioURI;
import alluxio.client.FileSystemTestUtils;
import alluxio.client.WriteType;
import alluxio.client.file.URIStatus;
import alluxio.shell.AbstractAlluxioShellTest;
import alluxio.shell.AlluxioShellUtilsTest;
import alluxio.util.io.PathUtils;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for setReplication command.
 */
public final class SetReplicationCommandTest extends AbstractAlluxioShellTest {
  @Test
  public void setReplicationMin() throws Exception {
    FileSystemTestUtils.createByteFile(mFileSystem, "/testFile", WriteType.MUST_CACHE, 10);
    mFsShell.run("setReplication", "-min", "1", "/testFile");

    URIStatus status = mFileSystem.getStatus(new AlluxioURI("/testFile"));
    Assert.assertEquals(1, status.getReplicationMin());
  }

  @Test
  public void setReplicationMax() throws Exception {
    FileSystemTestUtils.createByteFile(mFileSystem, "/testFile", WriteType.MUST_CACHE, 10);
    mFsShell.run("setReplication", "-max", "2", "/testFile");

    URIStatus status = mFileSystem.getStatus(new AlluxioURI("/testFile"));
    Assert.assertEquals(2, status.getReplicationMax());
  }

  @Test
  public void setReplicationMinMax() throws Exception {
    FileSystemTestUtils.createByteFile(mFileSystem, "/testFile", WriteType.MUST_CACHE, 10);
    mFsShell.run("setReplication", "-min", "1", "-max", "2", "/testFile");

    URIStatus status = mFileSystem.getStatus(new AlluxioURI("/testFile"));
    Assert.assertEquals(1, status.getReplicationMin());
    Assert.assertEquals(2, status.getReplicationMax());
  }

  @Test
  public void setReplicationNoMinMax() throws Exception {
    FileSystemTestUtils.createByteFile(mFileSystem, "/testFile", WriteType.MUST_CACHE, 10);
    int ret = mFsShell.run("setReplication", "/testFile");
    Assert.assertEquals(-1, ret);
  }

  @Test
  public void setReplicationBadMinMax() throws Exception {
    FileSystemTestUtils.createByteFile(mFileSystem, "/testFile", WriteType.MUST_CACHE, 10);
    int ret = mFsShell.run("setReplication", "-min", "2", "-max", "1", "/testFile");
    Assert.assertEquals(-1, ret);
  }

  @Test
  public void setReplicationBadMinMaxSeparately() throws Exception {
    FileSystemTestUtils.createByteFile(mFileSystem, "/testFile", WriteType.MUST_CACHE, 10);
    int ret = mFsShell.run("setReplication", "-min", "2", "/testFile");
    Assert.assertEquals(0, ret);
    ret = mFsShell.run("setReplication", "-max", "1", "/testFile");
    Assert.assertEquals(-1, ret);
  }

  @Test
  public void setReplicationRecursively() throws Exception {
    String testDir = AlluxioShellUtilsTest.resetFileHierarchy(mFileSystem);
    mFsShell.run("setReplication", "-R", "-min", "2", PathUtils.concatPath(testDir, "foo"));

    URIStatus status1 =
        mFileSystem.getStatus(new AlluxioURI(PathUtils.concatPath(testDir, "foo", "foobar1")));
    URIStatus status2 =
        mFileSystem.getStatus(new AlluxioURI(PathUtils.concatPath(testDir, "foo", "foobar2")));
    Assert.assertEquals(2, status1.getReplicationMin());
    Assert.assertEquals(2, status2.getReplicationMin());
  }
}
