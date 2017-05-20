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

package alluxio.underfs.fork;

import alluxio.underfs.UfsDirectoryStatus;
import alluxio.underfs.UfsFileStatus;
import alluxio.underfs.UfsStatus;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.util.io.PathUtils;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Integration tests for {@link ForkUnderFileSystem}.
 */
public class ForkUnderFileSystemIntegrationTest {
  @Rule
  public TemporaryFolder mUfsFolderA = new TemporaryFolder();
  @Rule
  public TemporaryFolder mUfsFolderB = new TemporaryFolder();

  private String mUfsPathA;
  private String mUfsPathB;
  private UnderFileSystem mUnderFileSystem;

  @Before
  public void before() {
    mUfsPathA = mUfsFolderA.getRoot().getAbsolutePath();
    mUfsPathB = mUfsFolderB.getRoot().getAbsolutePath();
    Map<String, String> properties = new HashMap<>();
    properties.put("alluxio-fork.A.ufs", mUfsPathA);
    properties.put("alluxio-fork.B.ufs", mUfsPathB);
    mUnderFileSystem = UnderFileSystem.Factory
        .create("alluxio-fork://", new UnderFileSystemConfiguration(false, false, properties));
  }

  @Test
  public void directoryLifeCycle() throws Exception {
    String[] dirnames = { "test-dir", "alluxio-fork:///test-dir"};
    for (String dirname : dirnames) {
      // Initially the file should not exist in either UFS.
      Assert.assertFalse(exists(PathUtils.concatPath(mUfsPathA, normalize(dirname))));
      Assert.assertFalse(exists(PathUtils.concatPath(mUfsPathB, normalize(dirname))));
      Assert.assertFalse(mUnderFileSystem.exists(dirname));
      // Create it and check it exists in both UFSes.
      Assert.assertTrue(mUnderFileSystem.mkdirs(dirname));
      Assert.assertTrue(exists(PathUtils.concatPath(mUfsPathA, normalize(dirname))));
      Assert.assertTrue(exists(PathUtils.concatPath(mUfsPathB, normalize(dirname))));
      Assert.assertTrue(mUnderFileSystem.exists(dirname));
      // Rename it and check it is renamed in both UFSes.
      String newDirname = dirname + "-new";
      Assert.assertTrue(mUnderFileSystem.renameDirectory(dirname, newDirname));
      Assert.assertTrue(exists(PathUtils.concatPath(mUfsPathA, normalize(newDirname))));
      Assert.assertTrue(exists(PathUtils.concatPath(mUfsPathB, normalize(newDirname))));
      Assert.assertTrue(mUnderFileSystem.exists(newDirname));
      Assert.assertFalse(exists(PathUtils.concatPath(mUfsPathA, normalize(dirname))));
      Assert.assertFalse(exists(PathUtils.concatPath(mUfsPathB, normalize(dirname))));
      Assert.assertFalse(mUnderFileSystem.exists(dirname));
      // Delete it and check it is deleted in both UFSes.
      Assert.assertTrue(mUnderFileSystem.deleteDirectory(newDirname));
      Assert.assertFalse(exists(PathUtils.concatPath(mUfsPathA, normalize(newDirname))));
      Assert.assertFalse(exists(PathUtils.concatPath(mUfsPathB, normalize(newDirname))));
      Assert.assertFalse(mUnderFileSystem.exists(newDirname));
    }
  }

  @Test
  public void fileLifeCycle() throws Exception {
    String[] filenames = { "test-file", "alluxio-fork:///test-file"};
    for (String filename : filenames) {
      // Initially the directory should not exist in either UFS.
      Assert.assertFalse(exists(PathUtils.concatPath(mUfsPathA, normalize(filename))));
      Assert.assertFalse(exists(PathUtils.concatPath(mUfsPathB, normalize(filename))));
      Assert.assertFalse(mUnderFileSystem.exists(filename));
      // Create it and check it is created in both UFSes.
      mUnderFileSystem.create(filename).close();
      Assert.assertTrue(exists(PathUtils.concatPath(mUfsPathA, normalize(filename))));
      Assert.assertTrue(exists(PathUtils.concatPath(mUfsPathB, normalize(filename))));
      Assert.assertTrue(mUnderFileSystem.exists(filename));
      // Rename it and check it is renamed in both UFSes
      String newFilename = filename + "-new";
      Assert.assertTrue(mUnderFileSystem.renameFile(filename, newFilename));
      Assert.assertTrue(exists(PathUtils.concatPath(mUfsPathA, normalize(newFilename))));
      Assert.assertTrue(exists(PathUtils.concatPath(mUfsPathB, normalize(newFilename))));
      Assert.assertTrue(mUnderFileSystem.exists(newFilename));
      Assert.assertFalse(exists(PathUtils.concatPath(mUfsPathA, normalize(filename))));
      Assert.assertFalse(exists(PathUtils.concatPath(mUfsPathB, normalize(filename))));
      Assert.assertFalse(mUnderFileSystem.exists(filename));
      // Delete it and check it is deleted in both UFSes.
      Assert.assertTrue(mUnderFileSystem.deleteFile(newFilename));
      Assert.assertFalse(exists(PathUtils.concatPath(mUfsPathA, normalize(newFilename))));
      Assert.assertFalse(exists(PathUtils.concatPath(mUfsPathB, normalize(newFilename))));
      Assert.assertFalse(mUnderFileSystem.exists(newFilename));
    }
  }

  @Test
  public void directoryStatus() throws Exception {
    String[] dirnames = { "test-dir", "alluxio-fork:///test-dir"};
    for (String dirname : dirnames) {
      Assert.assertTrue(mUnderFileSystem.mkdirs(dirname));
      Assert.assertTrue(mUnderFileSystem.isDirectory(dirname));
      Assert.assertFalse(mUnderFileSystem.isFile(dirname));
      UfsDirectoryStatus status = mUnderFileSystem.getDirectoryStatus(dirname);
      Assert.assertTrue(exists(PathUtils.concatPath(status.getName())));
      Assert.assertTrue(mUnderFileSystem.deleteDirectory(dirname));
    }
  }

  @Test
  public void fileStatus() throws Exception {
    String[] filenames = { "test-file", "alluxio-fork:///test-file"};
    for (String filename : filenames) {
      mUnderFileSystem.create(filename).close();
      Assert.assertFalse(mUnderFileSystem.isDirectory(filename));
      Assert.assertTrue(mUnderFileSystem.isFile(filename));
      UfsFileStatus status = mUnderFileSystem.getFileStatus(filename);
      Assert.assertTrue(exists(PathUtils.concatPath(status.getName())));
      Assert.assertTrue(mUnderFileSystem.deleteFile(filename));
    }
  }

  @Test
  public void listStatus() throws Exception {
    String dirname = "test-dir";
    Assert.assertTrue(mUnderFileSystem.mkdirs(dirname));
    String[] filenames = { "test-file1", "test-file2"};
    for (String filename : filenames) {
      mUnderFileSystem.create(PathUtils.concatPath(dirname, filename)).close();
    }
    UfsStatus[] statuses = mUnderFileSystem.listStatus(dirname);
    Assert.assertEquals(2, statuses.length);
  }

  @Test
  public void writeAndRead() throws Exception {
    String message = "Hello World!";
    String[] filenames = { "test-dir", "alluxio-fork:///test-dir"};
    for (String filename : filenames) {
      // Write a test file.
      OutputStream os = mUnderFileSystem.create(filename);
      os.write(message.getBytes());
      os.close();
      InputStream is = mUnderFileSystem.open(filename);
      int bufferLength = message.length() / 2;
      byte[] buffer = new byte[bufferLength];
      // Check that you can read the first half.
      Assert.assertEquals(bufferLength, is.read(buffer));
      Assert.assertArrayEquals(message.substring(0, bufferLength).getBytes(), buffer);
      // Remove one of the underlying files.
      boolean coinFlip = new Random().nextBoolean();
      if (coinFlip) {
        Assert.assertTrue(delete(PathUtils.concatPath(mUfsPathA, normalize(filename))));
      } else {
        Assert.assertTrue(delete(PathUtils.concatPath(mUfsPathB, normalize(filename))));
      }
      // Check that you can read the second half.
      Assert.assertEquals(bufferLength, is.read(buffer));
      Assert
          .assertArrayEquals(message.substring(bufferLength, 2 * bufferLength).getBytes(), buffer);
      // Check that all data has been read.
      Assert.assertEquals(-1, is.read(buffer));
      is.close();
      // Cleanup the remaining UFS file.
      if (!coinFlip) {
        Assert.assertTrue(delete(PathUtils.concatPath(mUfsPathA, normalize(filename))));
      } else {
        Assert.assertTrue(delete(PathUtils.concatPath(mUfsPathB, normalize(filename))));
      }
      Assert.assertFalse(mUnderFileSystem.exists(filename));
      Assert.assertFalse(exists(PathUtils.concatPath(mUfsPathA, normalize(filename))));
      Assert.assertFalse(exists(PathUtils.concatPath(mUfsPathB, normalize(filename))));
    }
  }

  private boolean delete(String path) {
    File f = new File(path);
    return f.delete();
  }

  private boolean exists(String path) {
    File f = new File(path);
    return f.exists();
  }

  private String normalize(String path) throws Exception {
    URI uri = new URI(path);
    return uri.getPath();
  }
}
