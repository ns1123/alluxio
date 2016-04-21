/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.move;

import alluxio.AlluxioURI;
import alluxio.client.WriteType;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.job.JobManagerIntegrationTest;

import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;

/**
 * Integration test for the move job.
 */
public final class MoveIntegrationTest extends JobManagerIntegrationTest {
  private static final byte[] TEST_BYTES = "hello".getBytes();

  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();

  /**
   * Tests moving a file within the same mount point.
   */
  @Test
  public void moveFileTest() throws Exception {
    String source = "/source";
    String destination = "/destination";
    createFile(source);
    long jobId = mJobManagerMaster
        .runJob(new MoveConfig(source, destination, WriteType.CACHE_THROUGH.toString(), true));
    waitForJobToFinish(jobId);
    Assert.assertFalse(mFileSystem.exists(new AlluxioURI(source)));
    Assert.assertTrue(mFileSystem.exists(new AlluxioURI(destination)));
    try (FileInStream in = mFileSystem.openFile(new AlluxioURI(destination))) {
      Assert.assertArrayEquals(TEST_BYTES, IOUtils.toByteArray(in));
    }
    // No tasks are needed when moving within the same mount point.
    Assert.assertEquals(0, mJobManagerMaster.getJobInfo(jobId).getTaskIdList().size());
  }

  /**
   * Tests moving a file between two mount points.
   */
  @Test
  public void crossMountMoveTest() throws Exception {
    File ufsMountPoint = tmpFolder.getRoot();
    mFileSystem.mount(new AlluxioURI("/mount"), new AlluxioURI(ufsMountPoint.getAbsolutePath()));
    String source = "/source";
    String destination = "/mount/destination";
    createFile(source);
    long jobId = mJobManagerMaster
        .runJob(new MoveConfig(source, destination, WriteType.CACHE_THROUGH.toString(), true));
    waitForJobToFinish(jobId);
    Assert.assertFalse(mFileSystem.exists(new AlluxioURI(source)));
    Assert.assertTrue(mFileSystem.exists(new AlluxioURI(destination)));
    try (FileInStream in = mFileSystem.openFile(new AlluxioURI(destination))) {
      Assert.assertArrayEquals(TEST_BYTES, IOUtils.toByteArray(in));
    }
    // One worker task is needed when moving within the same mount point.
    Assert.assertEquals(1, mJobManagerMaster.getJobInfo(jobId).getTaskIdList().size());
  }

  /**
   * Tests moving a directory between mount points.
   */
  @Test
  public void moveDirectoryTest() throws Exception {
    File ufsMountPoint = tmpFolder.getRoot();
    mFileSystem.mount(new AlluxioURI("/mount"), new AlluxioURI(ufsMountPoint.getAbsolutePath()));
    mFileSystem.createDirectory(new AlluxioURI("/source"));
    createFile("/source/foo");
    createFile("/source/bar");
    mFileSystem.createDirectory(new AlluxioURI("/source/baz"));
    createFile("/source/baz/bat");
    long jobId = mJobManagerMaster.runJob(
        new MoveConfig("/source", "/mount/destination", WriteType.CACHE_THROUGH.toString(), true));
    waitForJobToFinish(jobId);
    Assert.assertFalse(mFileSystem.exists(new AlluxioURI("/source")));
    Assert.assertTrue(mFileSystem.exists(new AlluxioURI("/mount/destination")));
    checkFile("/mount/destination/baz/bat");
  }

  /**
   * Creates a file with the given name containing TEST_BYTES.
   */
  private void createFile(String filename) throws Exception {
    try (FileOutStream out = mFileSystem.createFile(new AlluxioURI(filename))) {
      out.write(TEST_BYTES);
    }
  }

  /**
   * Checks that the given file contains TEST_BYTES.
   */
  private void checkFile(String filename) throws Exception {
    try (FileInStream in = mFileSystem.openFile(new AlluxioURI(filename))) {
      Assert.assertArrayEquals(TEST_BYTES, IOUtils.toByteArray(in));
    }
  }
}
