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

package alluxio.security;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.ConfigurationTestUtils;
import alluxio.LocalAlluxioClusterResource;
import alluxio.PropertyKey;
import alluxio.client.file.FileSystemMasterClient;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.security.authentication.AuthType;
import alluxio.security.minikdc.MiniKdc;

import com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

/**
 * Tests RPC authentication between master and its client, in Kerberos mode.
 */
// TODO(bin): improve the way to set and isolate MasterContext/WorkerContext across test cases
@Ignore("TODO(chaomin): debug this integration test failure and re-enable.")
public final class MasterClientKerberosIntegrationTest {

  private static MiniKdc sKdc;
  private static File sWorkDir;

  private static String sClientPrincipal;
  private static File sClientKeytab;
  private static String sServerPrincipal;
  private static File sServerKeytab;

  /**
   * Temporary folder for miniKDC keytab files.
   */
  @ClassRule
  public static final TemporaryFolder FOLDER = new TemporaryFolder();

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder().setStartCluster(false).build();

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @BeforeClass
  public static void beforeClass() throws Exception {
    sWorkDir = FOLDER.getRoot();
    sKdc = new MiniKdc(MiniKdc.createConf(), sWorkDir);
    sKdc.start();

    sClientPrincipal = "client/host@EXAMPLE.COM";
    sClientKeytab = new File(sWorkDir, "client.keytab");
    // Create a principal in miniKDC, and generate the keytab file for it.
    sKdc.createPrincipal(sClientKeytab, "client/host");

    sServerPrincipal = "server/host@EXAMPLE.COM";
    sServerKeytab = new File(sWorkDir, "server.keytab");
    // Create a principal in miniKDC, and generate the keytab file for it.
    sKdc.createPrincipal(sServerKeytab, "server/host");

    LoginUserTestUtils.resetLoginUser();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    if (sKdc != null) {
      sKdc.stop();
    }
  }

  @After
  public void after() throws Exception {
    LoginUserTestUtils.resetLoginUser();
    ConfigurationTestUtils.resetConfiguration();
  }

  /**
   * Tests Alluxio client and Master authentication, in Kerberos mode.
   */
  @Test
  public void kerberosAuthenticationOpenCloseTest() throws Exception {
    startTestClusterWithKerberos();
    authenticationOperationTest("/kerberos-file");
  }

  /**
   * Tests multiple Alluxio clients authentication, in Kerberos mode.
   */
  @Test
  public void kerberosAuthenticationMultipleUsersTest() throws Exception {
    startTestClusterWithKerberos();
    authenticationOperationTest("/kerberos-file-client");

    ConfigurationTestUtils.resetConfiguration();
    LoginUserTestUtils.resetLoginUser();

    // Switching to another login user mServer.
    Configuration.set(PropertyKey.SECURITY_KERBEROS_CLIENT_PRINCIPAL, sServerPrincipal);
    Configuration.set(PropertyKey.SECURITY_KERBEROS_CLIENT_KEYTAB_FILE, sServerKeytab.getPath());
    FileSystemMasterClient masterClient = new FileSystemMasterClient(
        mLocalAlluxioClusterResource.get().getMaster().getAddress());
    Assert.assertFalse(masterClient.isConnected());
    masterClient.connect();
    Assert.assertTrue(masterClient.isConnected());
    String newFilename = "/kerberos-file-server";
    masterClient.createFile(new AlluxioURI(newFilename), CreateFileOptions.defaults());
    Assert.assertNotNull(masterClient.getStatus(new AlluxioURI(newFilename)));
    masterClient.disconnect();
    masterClient.close();
  }

  /**
   * Tests Alluxio client and Master authentication, with empty client principal.
   */
  @Test
  public void kerberosAuthenticationWithEmptyPrincipalTest() throws Exception {
    startTestClusterWithKerberos();

    Configuration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.KERBEROS.getAuthName());
    Configuration.set(PropertyKey.SECURITY_KERBEROS_SERVER_PRINCIPAL, sServerPrincipal);
    Configuration.set(PropertyKey.SECURITY_KERBEROS_SERVER_KEYTAB_FILE, sServerKeytab.getPath());
    // Empty client principal.
    Configuration.set(PropertyKey.SECURITY_KERBEROS_CLIENT_PRINCIPAL, "");
    Configuration.set(PropertyKey.SECURITY_KERBEROS_CLIENT_KEYTAB_FILE, sClientKeytab.getPath());
    FileSystemMasterClient masterClient = new FileSystemMasterClient(
        mLocalAlluxioClusterResource.get().getMaster().getAddress());
    Assert.assertFalse(masterClient.isConnected());
    mThrown.expect(IOException.class);
    masterClient.connect();
    masterClient.close();
  }

  /**
   * Tests Alluxio client and Master authentication, with empty client keytab file.
   */
  @Test
  public void kerberosAuthenticationWithEmptyKeytabTest() throws Exception {
    startTestClusterWithKerberos();

    Configuration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.KERBEROS.getAuthName());
    Configuration.set(PropertyKey.SECURITY_KERBEROS_SERVER_PRINCIPAL, sServerPrincipal);
    Configuration.set(PropertyKey.SECURITY_KERBEROS_SERVER_KEYTAB_FILE, sServerKeytab.getPath());
    Configuration.set(PropertyKey.SECURITY_KERBEROS_CLIENT_PRINCIPAL, sClientPrincipal);
    // Empty keytab file config.
    Configuration.set(PropertyKey.SECURITY_KERBEROS_CLIENT_KEYTAB_FILE, "");
    FileSystemMasterClient masterClient = new FileSystemMasterClient(
        mLocalAlluxioClusterResource.get().getMaster().getAddress());
    Assert.assertFalse(masterClient.isConnected());
    mThrown.expect(IOException.class);
    masterClient.connect();
    masterClient.close();
  }

  /**
   * Tests Alluxio client and Master authentication, with wrong client keytab file.
   */
  @Test
  public void kerberosAuthenticationWithWrongKeytabTest() throws Exception {
    startTestClusterWithKerberos();

    Configuration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.KERBEROS.getAuthName());
    Configuration.set(PropertyKey.SECURITY_KERBEROS_SERVER_PRINCIPAL, sServerPrincipal);
    Configuration.set(PropertyKey.SECURITY_KERBEROS_SERVER_KEYTAB_FILE, sServerKeytab.getPath());
    Configuration.set(PropertyKey.SECURITY_KERBEROS_CLIENT_PRINCIPAL, sClientPrincipal);
    // Wrong keytab file which does not contain the actual client principal credentials.
    Configuration.set(PropertyKey.SECURITY_KERBEROS_CLIENT_KEYTAB_FILE, sServerKeytab.getPath());

    FileSystemMasterClient masterClient = new FileSystemMasterClient(
        mLocalAlluxioClusterResource.get().getMaster().getAddress());
    Assert.assertFalse(masterClient.isConnected());
    mThrown.expect(IOException.class);
    masterClient.connect();
    masterClient.close();
  }

  /**
   * Starts the local testing cluster with Kerberos security enabled.
   */
  private void startTestClusterWithKerberos() throws Exception {
    mLocalAlluxioClusterResource.addProperties(ImmutableMap.<PropertyKey, Object>builder()
        .put(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.KERBEROS.getAuthName())
        .put(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, "true")
        .put(PropertyKey.SECURITY_KERBEROS_CLIENT_PRINCIPAL, sClientPrincipal)
        .put(PropertyKey.SECURITY_KERBEROS_CLIENT_KEYTAB_FILE, sClientKeytab.getPath())
        .put(PropertyKey.SECURITY_KERBEROS_SERVER_PRINCIPAL, sServerPrincipal)
        .put(PropertyKey.SECURITY_KERBEROS_SERVER_KEYTAB_FILE, sServerKeytab.getPath())
        .build());
    mLocalAlluxioClusterResource.start();
  }

  /**
   * Tests Alluxio client connects or disconnects to the Master. When the client connects
   * successfully to the Master, it can successfully create file or not.
   */
  private void authenticationOperationTest(String filename) throws Exception {
    FileSystemMasterClient masterClient =
        new FileSystemMasterClient(mLocalAlluxioClusterResource.get().getMaster().getAddress());
    Assert.assertFalse(masterClient.isConnected());
    masterClient.connect();
    Assert.assertTrue(masterClient.isConnected());
    masterClient.createFile(new AlluxioURI(filename), CreateFileOptions.defaults());
    Assert.assertNotNull(masterClient.getStatus(new AlluxioURI(filename)));
    masterClient.disconnect();
    masterClient.close();
  }
}
