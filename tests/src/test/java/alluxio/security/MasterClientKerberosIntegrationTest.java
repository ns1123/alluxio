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
import alluxio.Constants;
import alluxio.LocalAlluxioClusterResource;
import alluxio.client.file.FileSystemMasterClient;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.security.authentication.AuthType;
import alluxio.security.minikdc.MiniKdc;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
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
public final class MasterClientKerberosIntegrationTest {
  private MiniKdc mKdc;
  private File mWorkDir;

  private String mClientPrincipal;
  private File mClientKeytab;
  private String mServerPrincipal;
  private File mServerKeytab;

  /**
   * Temporary folder for miniKDC keytab files.
   */
  @Rule
  public final TemporaryFolder mFolder = new TemporaryFolder();

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource();

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  /**
   * Starts miniKDC and executor service.
   */
  @Before
  public void before() throws Exception {
    mWorkDir = mFolder.getRoot();
    mKdc = new MiniKdc(MiniKdc.createConf(), mWorkDir);
    mKdc.start();

    mClientPrincipal = "client/host@EXAMPLE.COM";
    mClientKeytab = new File(mWorkDir, "client.keytab");
    // Create a principal in miniKDC, and generate the keytab file for it.
    mKdc.createPrincipal(mClientKeytab, "client/host");

    mServerPrincipal = "server/host@EXAMPLE.COM";
    mServerKeytab = new File(mWorkDir, "server.keytab");
    // Create a principal in miniKDC, and generate the keytab file for it.
    mKdc.createPrincipal(mServerKeytab, "server/host");

    clearLoginUser();
  }

  /**
   * Stops miniKDC and executor service.
   */
  @After
  public void after() throws Exception {
    if (mKdc != null) {
      mKdc.stop();
    }
    clearLoginUser();
  }

  /**
   * Tests Alluxio client and Master authentication, in Kerberos mode.
   */
  @Test
  @LocalAlluxioClusterResource.Config(startCluster = false)
  public void kerberosAuthenticationOpenCloseTest() throws Exception {
    startTestClusterWithKerberos();
    authenticationOperationTest("/kerberos-file");
  }

  /**
   * Tests multiple Alluxio clients authentication, in Kerberos mode.
   */
  @Test
  @LocalAlluxioClusterResource.Config(startCluster = false)
  public void kerberosAuthenticationMultipleUsersTest() throws Exception {
    startTestClusterWithKerberos();
    authenticationOperationTest("/kerberos-file-client");

    LoginUserTestUtils.resetLoginUser();

    Configuration conf = mLocalAlluxioClusterResource.get().getMasterConf();
    // Switching to another login user mServer.
    conf.set(Constants.SECURITY_KERBEROS_CLIENT_PRINCIPAL, mServerPrincipal);
    conf.set(Constants.SECURITY_KERBEROS_CLIENT_KEYTAB_FILE, mServerKeytab.getPath());
    FileSystemMasterClient masterClient = new FileSystemMasterClient(
        mLocalAlluxioClusterResource.get().getMaster().getAddress(), conf);
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
  @LocalAlluxioClusterResource.Config(startCluster = false)
  public void kerberosAuthenticationWithEmptyPrincipalTest() throws Exception {
    startTestClusterWithKerberos();

    Configuration conf = new Configuration();
    conf.set(Constants.SECURITY_AUTHENTICATION_TYPE, AuthType.KERBEROS.getAuthName());
    conf.set(Constants.SECURITY_KERBEROS_SERVER_PRINCIPAL, mServerPrincipal);
    conf.set(Constants.SECURITY_KERBEROS_SERVER_KEYTAB_FILE, mServerKeytab.getPath());
    // Empty client principal.
    conf.set(Constants.SECURITY_KERBEROS_CLIENT_PRINCIPAL, "");
    conf.set(Constants.SECURITY_KERBEROS_CLIENT_KEYTAB_FILE, mClientKeytab.getPath());
    FileSystemMasterClient masterClient = new FileSystemMasterClient(
        mLocalAlluxioClusterResource.get().getMaster().getAddress(), conf);
    Assert.assertFalse(masterClient.isConnected());
    mThrown.expect(IOException.class);
    masterClient.connect();
  }

  /**
   * Tests Alluxio client and Master authentication, with empty client keytab file.
   */
  @Test
  @LocalAlluxioClusterResource.Config(startCluster = false)
  public void kerberosAuthenticationWithEmptyKeytabTest() throws Exception {
    startTestClusterWithKerberos();

    Configuration conf = new Configuration();
    conf.set(Constants.SECURITY_AUTHENTICATION_TYPE, AuthType.KERBEROS.getAuthName());
    conf.set(Constants.SECURITY_KERBEROS_SERVER_PRINCIPAL, mServerPrincipal);
    conf.set(Constants.SECURITY_KERBEROS_SERVER_KEYTAB_FILE, mServerKeytab.getPath());
    conf.set(Constants.SECURITY_KERBEROS_CLIENT_PRINCIPAL, mClientPrincipal);
    // Empty keytab file config.
    conf.set(Constants.SECURITY_KERBEROS_CLIENT_KEYTAB_FILE, "");
    FileSystemMasterClient masterClient = new FileSystemMasterClient(
        mLocalAlluxioClusterResource.get().getMaster().getAddress(), conf);
    Assert.assertFalse(masterClient.isConnected());
    mThrown.expect(IOException.class);
    masterClient.connect();
  }

  /**
   * Tests Alluxio client and Master authentication, with wrong client keytab file.
   */
  @Test
  @LocalAlluxioClusterResource.Config(startCluster = false)
  public void kerberosAuthenticationWithWrongKeytabTest() throws Exception {
    startTestClusterWithKerberos();

    Configuration conf = new Configuration();
    conf.set(Constants.SECURITY_AUTHENTICATION_TYPE, AuthType.KERBEROS.getAuthName());
    conf.set(Constants.SECURITY_KERBEROS_SERVER_PRINCIPAL, mServerPrincipal);
    conf.set(Constants.SECURITY_KERBEROS_SERVER_KEYTAB_FILE, mServerKeytab.getPath());
    conf.set(Constants.SECURITY_KERBEROS_CLIENT_PRINCIPAL, mClientPrincipal);
    // Wrong keytab file which does not contain the actual client principal credentials.
    conf.set(Constants.SECURITY_KERBEROS_CLIENT_KEYTAB_FILE, mServerKeytab.getPath());

    FileSystemMasterClient masterClient = new FileSystemMasterClient(
        mLocalAlluxioClusterResource.get().getMaster().getAddress(), conf);
    Assert.assertFalse(masterClient.isConnected());
    mThrown.expect(IOException.class);
    masterClient.connect();
  }

  /**
   * Starts the local testing cluster with Kerberos security enabled.
   */
  private void startTestClusterWithKerberos() throws Exception {
    mLocalAlluxioClusterResource.addConfParams(
        new String[] {
            Constants.SECURITY_AUTHENTICATION_TYPE, AuthType.KERBEROS.getAuthName(),
            Constants.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, "true",
            Constants.SECURITY_KERBEROS_CLIENT_PRINCIPAL, mClientPrincipal,
            Constants.SECURITY_KERBEROS_CLIENT_KEYTAB_FILE, mClientKeytab.getPath(),
            Constants.SECURITY_KERBEROS_SERVER_PRINCIPAL, mServerPrincipal,
            Constants.SECURITY_KERBEROS_SERVER_KEYTAB_FILE, mServerKeytab.getPath() }
    );
    mLocalAlluxioClusterResource.start();
  }

  /**
   * Tests Alluxio client connects or disconnects to the Master. When the client connects
   * successfully to the Master, it can successfully create file or not.
   */
  private void authenticationOperationTest(String filename) throws Exception {
    FileSystemMasterClient masterClient =
        new FileSystemMasterClient(mLocalAlluxioClusterResource.get().getMaster().getAddress(),
            mLocalAlluxioClusterResource.get().getMasterConf());
    Assert.assertFalse(masterClient.isConnected());
    masterClient.connect();
    Assert.assertTrue(masterClient.isConnected());
    masterClient.createFile(new AlluxioURI(filename), CreateFileOptions.defaults());
    Assert.assertNotNull(masterClient.getStatus(new AlluxioURI(filename)));
    masterClient.disconnect();
    masterClient.close();
  }

  private void clearLoginUser() throws Exception {
    LoginUserTestUtils.resetLoginUser();
  }
}
