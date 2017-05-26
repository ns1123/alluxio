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

import alluxio.AlluxioURI;
import alluxio.LocalAlluxioClusterResource;
import alluxio.client.WriteType;
import alluxio.client.file.FileSystem;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.options.MountOptions;
import alluxio.util.io.PathUtils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

/**
 * Interface tests for {@link ForkUnderFileSystem}.
 */
public class ForkUnderFileSystemIntegrationTest {
  @Rule
  public TemporaryFolder mUfsFolderA = new TemporaryFolder();
  @Rule
  public TemporaryFolder mUfsFolderB = new TemporaryFolder();
  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder().build();

  private String mUfsPathA;
  private String mUfsPathB;
  private FileSystem mFileSystem;

  @After
  public void after() throws Exception {}

  @Before
  public void before() throws Exception {
    mUfsPathA = mUfsFolderA.getRoot().getAbsolutePath();
    mUfsPathB = mUfsFolderB.getRoot().getAbsolutePath();
    mFileSystem = FileSystem.Factory.get();
  }

  @Test
  public void fileLifeCycle() throws Exception {
    mFileSystem.createDirectory(new AlluxioURI("/mnt"));
    Map<String, String> properties = new HashMap<>();
    properties.put("alluxio-fork.A.ufs", mUfsPathA);
    properties.put("alluxio-fork.B.ufs", mUfsPathB);
    mFileSystem.mount(new AlluxioURI("/mnt/test"), new AlluxioURI("alluxio-fork://test/"),
        MountOptions.defaults().setProperties(properties));
    String filename = "test-file";
    AlluxioURI uri = new AlluxioURI("/mnt/test/test-file");
    // Initially the file should not exist in either UFS.
    Assert.assertFalse(exists(PathUtils.concatPath(mUfsPathA, filename)));
    Assert.assertFalse(exists(PathUtils.concatPath(mUfsPathB, filename)));
    Assert.assertFalse(mFileSystem.exists(uri));
    // Create it with CACHE_THROUGH and check it exists in both UFSes.
    touch(uri);
    Assert.assertTrue(exists(PathUtils.concatPath(mUfsPathA, filename)));
    Assert.assertTrue(exists(PathUtils.concatPath(mUfsPathB, filename)));
    Assert.assertTrue(mFileSystem.exists(uri));
    // Rename it and check it is renamed in both UFSes.
    String newFilename = "test-file-new";
    AlluxioURI newUri = new AlluxioURI("/mnt/test/test-file-new");
    mFileSystem.rename(uri, newUri);
    Assert.assertTrue(exists(PathUtils.concatPath(mUfsPathA, newFilename)));
    Assert.assertTrue(exists(PathUtils.concatPath(mUfsPathB, newFilename)));
    Assert.assertTrue(mFileSystem.exists(newUri));
    Assert.assertFalse(exists(PathUtils.concatPath(mUfsPathA, filename)));
    Assert.assertFalse(exists(PathUtils.concatPath(mUfsPathB, filename)));
    Assert.assertFalse(mFileSystem.exists(uri));
    // Delete it and check it is deleted in both UFSes.
    mFileSystem.delete(newUri);
    Assert.assertFalse(exists(PathUtils.concatPath(mUfsPathA, newFilename)));
    Assert.assertFalse(exists(PathUtils.concatPath(mUfsPathB, newFilename)));
    Assert.assertFalse(mFileSystem.exists(newUri));
  }

  @Test
  public void multiMount() throws Exception {
    mFileSystem.createDirectory(new AlluxioURI("/mnt"));
    Map<String, String> propertiesA = new HashMap<>();
    propertiesA.put("alluxio-fork.A.ufs", mUfsPathA);
    Map<String, String> propertiesB = new HashMap<>();
    propertiesB.put("alluxio-fork.B.ufs", mUfsPathB);
    mFileSystem.mount(new AlluxioURI("/mnt/A"), new AlluxioURI("alluxio-fork://A/"),
        MountOptions.defaults().setProperties(propertiesA));
    mFileSystem.mount(new AlluxioURI("/mnt/B"), new AlluxioURI("alluxio-fork://B/"),
        MountOptions.defaults().setProperties(propertiesB));
    String filename = "test-file";
    AlluxioURI uriA = new AlluxioURI("/mnt/A/test-file");
    AlluxioURI uriB = new AlluxioURI("/mnt/B/test-file");
    // Initially the file should not exist in either UFS or mount point.
    Assert.assertFalse(exists(PathUtils.concatPath(mUfsPathA, filename)));
    Assert.assertFalse(exists(PathUtils.concatPath(mUfsPathB, filename)));
    Assert.assertFalse(mFileSystem.exists(uriA));
    Assert.assertFalse(mFileSystem.exists(uriB));
    // Create it under one mount point with CACHE_THROUGH and check it exists in the matching UFS.
    touch(uriA);
    Assert.assertTrue(exists(PathUtils.concatPath(mUfsPathA, filename)));
    Assert.assertFalse(exists(PathUtils.concatPath(mUfsPathB, filename)));
    Assert.assertTrue(mFileSystem.exists(uriA));
    Assert.assertFalse(mFileSystem.exists(uriB));
    // Create it under the other mount point with CACHE_THROUGH and check it exists in both UFSes.
    touch(uriB);
    Assert.assertTrue(exists(PathUtils.concatPath(mUfsPathA, filename)));
    Assert.assertTrue(exists(PathUtils.concatPath(mUfsPathB, filename)));
    Assert.assertTrue(mFileSystem.exists(uriA));
    Assert.assertTrue(mFileSystem.exists(uriB));
  }

  private boolean exists(String path) {
    File f = new File(path);
    return f.exists();
  }

  private void touch(AlluxioURI uri) throws Exception {
    CreateFileOptions options = CreateFileOptions.defaults().setWriteType(WriteType.CACHE_THROUGH);
    mFileSystem.createFile(uri, options).close();
  }
}
