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

package alluxio.client;

import static org.hamcrest.CoreMatchers.containsString;

import alluxio.AlluxioURI;
import alluxio.LocalAlluxioClusterResource;
import alluxio.PropertyKey;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.options.CreateDirectoryOptions;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.master.LocalAlluxioCluster;
import alluxio.security.LoginUserTestUtils;
import alluxio.security.authorization.Mode;
import alluxio.util.io.PathUtils;
import alluxio.worker.block.BlockWorker;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;

/**
 * Integration tests on Alluxio Client (reuse the {@link LocalAlluxioCluster}).
 */
public final class DataAuthorizationIntegrationTest {
  private static final int USER_QUOTA_UNIT_BYTES = 1000;
  private static final String TMP_DIR = "/tmp";

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder()
          .setProperty(PropertyKey.USER_FILE_BUFFER_BYTES, Integer.toString(USER_QUOTA_UNIT_BYTES))
          .setProperty(PropertyKey.SECURITY_AUTHORIZATION_CAPABILITY_ENABLED, true)
          .build();
  private FileSystem mFileSystem = null;

  @Before
  public void before() throws Exception {
    mFileSystem = mLocalAlluxioClusterResource.get().getClient();
    mFileSystem.createDirectory(new AlluxioURI(TMP_DIR),
        CreateDirectoryOptions.defaults().setMode(Mode.createFullAccess()));
    FileSystemContext.INSTANCE.reset();
    LoginUserTestUtils.resetLoginUser("test");
  }

  @After
  public void after() throws Exception {
    FileSystemContext.INSTANCE.reset();
    LoginUserTestUtils.resetLoginUser();
  }

  @Test
  public void createFile() throws Exception {
    String uniqPath = TMP_DIR + PathUtils.uniqPath();
    AlluxioURI uri = new AlluxioURI(uniqPath);
    Mode mode = Mode.defaults();
    mode.fromShort((short) 0600);
    CreateFileOptions options =
        CreateFileOptions.defaults().setMode(mode).setWriteType(WriteType.MUST_CACHE);
    try (FileOutStream outStream = mFileSystem.createFile(uri, options)) {
      outStream.write(1);
    }
  }

  @Test
  public void expiredCapability() throws Exception {
    String uniqPath = TMP_DIR + PathUtils.uniqPath();
    AlluxioURI uri = new AlluxioURI(uniqPath);
    Mode mode = Mode.defaults();
    mode.fromShort((short) 0600);
    CreateFileOptions options =
        CreateFileOptions.defaults().setMode(mode).setWriteType(WriteType.MUST_CACHE)
            .setBlockSizeBytes(8);
    try (FileOutStream outStream = mFileSystem.createFile(uri, options)) {
      outStream.write(1);
      mLocalAlluxioClusterResource.get().getWorkerProcess().getWorker(BlockWorker.class)
          .getCapabilityCache().expireCapabilityForUser("test");
      for (int i = 0; i < 32; i++) {
        outStream.write(1);
      }
    }
  }

  @Test
  @LocalAlluxioClusterResource.Config(
      confParams = {PropertyKey.Name.SECURITY_AUTHORIZATION_CAPABILITY_LIFETIME_MS, "-100"})
  public void expiredCapabilityForever() throws Exception {
    String uniqPath = TMP_DIR + PathUtils.uniqPath();
    AlluxioURI uri = new AlluxioURI(uniqPath);
    Mode mode = Mode.defaults();
    mode.fromShort((short) 0600);
    CreateFileOptions options =
        CreateFileOptions.defaults().setMode(mode).setWriteType(WriteType.MUST_CACHE);
    try (FileOutStream outStream = mFileSystem.createFile(uri, options)) {
      outStream.write(1);
      Assert.fail();
    } catch (IOException e) {
      Assert.assertThat(e.getMessage(), containsString("capability"));
      Assert.assertThat(e.getMessage(), containsString("expired"));
    }
  }

  @Test
  public void readFile() throws Exception {
    String uniqPath = TMP_DIR + PathUtils.uniqPath();
    AlluxioURI uri = new AlluxioURI(uniqPath);
    Mode mode = Mode.defaults();
    mode.fromShort((short) 0600);
    CreateFileOptions options =
        CreateFileOptions.defaults().setMode(mode).setWriteType(WriteType.MUST_CACHE);
    try (FileOutStream outStream = mFileSystem.createFile(uri, options)) {
      outStream.write(1);
    }

    try (FileInStream instream = mFileSystem.openFile(uri)) {
      instream.read();
    }
  }

  @Test
  public void readFileNoPermission() throws Exception {
    String uniqPath = TMP_DIR + PathUtils.uniqPath();
    AlluxioURI uri = new AlluxioURI(uniqPath);
    Mode mode = Mode.defaults();
    mode.fromShort((short) 0600);
    CreateFileOptions options =
        CreateFileOptions.defaults().setMode(mode).setWriteType(WriteType.MUST_CACHE);
    try (FileOutStream outStream = mFileSystem.createFile(uri, options)) {
      outStream.write(1);
    }

    try (FileInStream instream = mFileSystem.openFile(uri)) {
      FileSystemContext.INSTANCE.reset();
      LoginUserTestUtils.resetLoginUser("test2");
      // We must reset client pool here so that all the existing connections are closed so that
      // the new user can be picked up.
      FileSystemContext.INSTANCE.reset();
      instream.read();
      Assert.fail();
    } catch (Exception e) {
      // read is expected to fail because test2 does not have permission to access the block
    }
  }
}
