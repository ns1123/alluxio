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

import alluxio.AlluxioURI;
import alluxio.LocalAlluxioClusterResource;
import alluxio.PropertyKey;
import alluxio.client.block.AlluxioBlockStore;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.options.CreateDirectoryOptions;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.exception.InvalidCapabilityException;
import alluxio.master.LocalAlluxioCluster;
import alluxio.security.LoginUserTestUtils;
import alluxio.security.authentication.AuthType;
import alluxio.security.authorization.Mode;
import alluxio.security.minikdc.MiniKdc;
import alluxio.util.CommonUtils;
import alluxio.util.io.PathUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.worker.block.BlockWorker;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;

/**
 * Integration tests on Alluxio Client (reuse the {@link LocalAlluxioCluster}).
 */
@Ignore("TODO(chaomin): fix this test")
public final class DataAuthorizationKerberosIntegrationTest {
  private static final String TMP_DIR = "/tmp";
  private static final String HOSTNAME = NetworkAddressUtils.getLocalHostName();

  private static MiniKdc sKdc;
  private static File sWorkDir;

  private static String sServerPrincipal;
  private static File sServerKeytab;

  @ClassRule
  public static final TemporaryFolder FOLDER = new TemporaryFolder();

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder()
          .setProperty(PropertyKey.MASTER_HOSTNAME, HOSTNAME)
          .setProperty(PropertyKey.WORKER_HOSTNAME, HOSTNAME)
          .setProperty(PropertyKey.SECURITY_AUTHORIZATION_CAPABILITY_ENABLED, true)
          .setProperty(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.KERBEROS.getAuthName())
          .setProperty(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, "true")
          .setProperty(PropertyKey.SECURITY_KERBEROS_CLIENT_PRINCIPAL, sServerPrincipal)
          .setProperty(PropertyKey.SECURITY_KERBEROS_CLIENT_KEYTAB_FILE, sServerKeytab.getPath())
          .setProperty(PropertyKey.SECURITY_KERBEROS_SERVER_PRINCIPAL, sServerPrincipal)
          .setProperty(PropertyKey.SECURITY_KERBEROS_SERVER_KEYTAB_FILE, sServerKeytab.getPath())
          .setProperty(PropertyKey.SECURITY_KERBEROS_SERVICE_NAME, "alluxio")
          .build();

  private FileSystem mFileSystem = null;

  @BeforeClass
  public static void beforeClass() throws Exception {
    sWorkDir = FOLDER.getRoot();
    sKdc = new MiniKdc(MiniKdc.createConf(), sWorkDir);
    sKdc.start();

    String realm = sKdc.getRealm();

    sServerPrincipal = "alluxio/" + HOSTNAME + "@" + realm;
    sServerKeytab = new File(sWorkDir, "alluxio.keytab");
    // Create a principal in miniKDC, and generate the keytab file for it.
    sKdc.createPrincipal(sServerKeytab, "alluxio/" + HOSTNAME);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    if (sKdc != null) {
      sKdc.stop();
    }
  }

  @Before
  public void before() throws Exception {
    mFileSystem = mLocalAlluxioClusterResource.get().getClient();
    mFileSystem.createDirectory(new AlluxioURI(TMP_DIR),
        CreateDirectoryOptions.defaults().setMode(Mode.createFullAccess()));
    FileSystemContext.INSTANCE.reset();
    LoginUserTestUtils.resetLoginUser();
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
          .getCapabilityCache().expireCapabilityForUser("alluxio");
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
    } catch (Exception e) {
      Assert.assertTrue(CommonUtils.getRootCause(e) instanceof InvalidCapabilityException);
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
  public void remoteIO() throws Exception {
    AlluxioBlockStore.create().setLocalHostName("fake");
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
}
