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

package alluxio.client.fs;

import alluxio.AlluxioURI;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.status.PermissionDeniedException;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.WritePType;
import alluxio.security.authentication.AuthType;
import alluxio.security.authorization.Mode;
import alluxio.security.minikdc.MiniKdc;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.util.CommonUtils;
import alluxio.util.io.PathUtils;
import alluxio.util.network.NetworkAddressUtils;

import com.google.common.collect.ImmutableMap;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;

/**
 * Integration tests on data authorization failures with Kerberos.
 */
public final class DataAuthorizationKerberosNegativeIntegrationTest extends BaseIntegrationTest {
  private static final String TMP_DIR = "/tmp";
  private static final String HOSTNAME = NetworkAddressUtils.getLocalHostName(
      (int) ServerConfiguration.getMs(PropertyKey.NETWORK_HOST_RESOLUTION_TIMEOUT_MS));
  private static final String UNIFIED_INSTANCE = "instance";

  private static MiniKdc sKdc;
  private static File sWorkDir;

  private static String sServerPrincipal;
  private static File sServerKeytab;

  @ClassRule
  public static final TemporaryFolder FOLDER = new TemporaryFolder();

  @BeforeClass
  public static void beforeClass() throws Exception {
    sWorkDir = FOLDER.getRoot();
    sKdc = new MiniKdc(MiniKdc.createConf(), sWorkDir);
    sKdc.start();

    String realm = sKdc.getRealm();

    sServerPrincipal = "alluxio/" + UNIFIED_INSTANCE + "@" + realm;
    sServerKeytab = new File(sWorkDir, "alluxio.keytab");
    // Create a principal in miniKDC, and generate the keytab file for it.
    sKdc.createPrincipal(sServerKeytab, "alluxio/" + UNIFIED_INSTANCE);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    if (sKdc != null) {
      sKdc.stop();
    }
  }

  @Test
  public void expiredCapabilityForever() throws Exception {
    LocalAlluxioClusterResource localAlluxioClusterResource =
        new LocalAlluxioClusterResource.Builder().setStartCluster(false).build();

    localAlluxioClusterResource.addProperties(ImmutableMap.<PropertyKey, Object>builder()
        .put(PropertyKey.MASTER_HOSTNAME, HOSTNAME)
        .put(PropertyKey.WORKER_HOSTNAME, HOSTNAME)
        .put(PropertyKey.SECURITY_AUTHORIZATION_CAPABILITY_ENABLED, true)
        .put(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.KERBEROS.getAuthName())
        .put(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, true)
        .put(PropertyKey.SECURITY_KERBEROS_CLIENT_PRINCIPAL, sServerPrincipal)
        .put(PropertyKey.SECURITY_KERBEROS_CLIENT_KEYTAB_FILE, sServerKeytab.getPath())
        .put(PropertyKey.SECURITY_KERBEROS_SERVER_PRINCIPAL, sServerPrincipal)
        .put(PropertyKey.SECURITY_KERBEROS_SERVER_KEYTAB_FILE, sServerKeytab.getPath())
        .put(PropertyKey.SECURITY_KERBEROS_SERVICE_NAME, "alluxio")
        .put(PropertyKey.SECURITY_KERBEROS_UNIFIED_INSTANCE_NAME, UNIFIED_INSTANCE)
        .put(PropertyKey.SECURITY_AUTHORIZATION_CAPABILITY_LIFETIME_MS, "-100")
        .build());
    localAlluxioClusterResource.start();

    FileSystem fileSystem = localAlluxioClusterResource.get().getClient();
    fileSystem.createDirectory(new AlluxioURI(TMP_DIR), CreateDirectoryPOptions.newBuilder()
        .setMode(Mode.createFullAccess().toProto()).setRecursive(true).build());

    String uniqPath = TMP_DIR + PathUtils.uniqPath();
    AlluxioURI uri = new AlluxioURI(uniqPath);
    Mode mode = Mode.defaults();
    mode.fromShort((short) 0600);
    CreateFilePOptions options = CreateFilePOptions.newBuilder().setMode(mode.toProto())
        .setWriteType(WritePType.MUST_CACHE).setRecursive(true).build();
    try (FileOutStream outStream = fileSystem.createFile(uri, options)) {
      outStream.write(1);
      Assert.fail();
    } catch (Exception e) {
      Assert.assertTrue(CommonUtils.getRootCause(e) instanceof PermissionDeniedException);
    }
  }
}
