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

import alluxio.Configuration;
import alluxio.ConfigurationTestUtils;
import alluxio.LocalAlluxioClusterResource;
import alluxio.PropertyKey;
import alluxio.client.block.BlockWorkerClient;
import alluxio.client.block.RetryHandlingBlockWorkerClient;
import alluxio.security.authentication.AuthType;
import alluxio.security.minikdc.MiniKdc;
import alluxio.util.ShellUtils;

import com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Tests RPC authentication between worker and its client, in Kerberos mode.
 */
// TODO(bin): improve the way to set and isolate MasterContext/WorkerContext across test cases
// TODO(chaomin, peis): Reset the BlockWorkerThriftClientPool when we start the local clusters.
@Ignore("TODO(chaomin): debug this integration test failure and re-enable.")
public final class BlockWorkerClientKerberosIntegrationTest {
  private static MiniKdc sKdc;
  private static File sWorkDir;

  private static String sClientPrincipal;
  private static File sClientKeytab;
  private static String sServerPrincipal;
  private static File sServerKeytab;

  @ClassRule
  public static final TemporaryFolder FOLDER = new TemporaryFolder();

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder().setStartCluster(false).build();
  private ExecutorService mExecutorService;

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
  }

  @AfterClass
  public static void afterClass() throws Exception {
    if (sKdc != null) {
      sKdc.stop();
    }
  }

  @Before
  public void before() throws Exception {
    mExecutorService = Executors.newFixedThreadPool(2);
    // Cleanup login user and Kerberos login ticket cache before each test case.
    // This is required because uncleared login user or Kerberos ticket cache would affect the login
    // result in later test cases.
    clearLoginUser();
    cleanUpTicketCache();
  }

  @After
  public void after() throws Exception {
    clearLoginUser();
    mExecutorService.shutdownNow();
    ConfigurationTestUtils.resetConfiguration();
  }

  /**
   * Tests Alluxio Worker client authentication in Kerberos mode.
   */
  @Test
  public void kerberosAuthenticationOpenCloseTest() throws Exception {
    startTestClusterWithKerberos();
    authenticationOperationTest();
  }

  /**
   * Tests multiple Alluxio Worker clients authentication, in Kerberos mode.
   */
  @Test
  @Ignore("TODO(chaomin): investigate why this test case is taking a long time.")
  public void kerberosAuthenticationMultipleUsersTest() throws Exception {
    startTestClusterWithKerberos();
    authenticationOperationTest();

    ConfigurationTestUtils.resetConfiguration();
    // Cleared login user and Kerberos ticket cache from previous login to prevent login
    // pollution in the following test.
    clearLoginUser();
    cleanUpTicketCache();

    Configuration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.KERBEROS.getAuthName());
    Configuration.set(PropertyKey.SECURITY_KERBEROS_SERVER_PRINCIPAL, sServerPrincipal);
    Configuration.set(PropertyKey.SECURITY_KERBEROS_SERVER_KEYTAB_FILE, sServerKeytab.getPath());
    // Switching to another login user mServer.
    Configuration.set(PropertyKey.SECURITY_KERBEROS_CLIENT_PRINCIPAL, sServerPrincipal);
    Configuration.set(PropertyKey.SECURITY_KERBEROS_CLIENT_KEYTAB_FILE, sServerKeytab.getPath());

    boolean isConnected;
    try (BlockWorkerClient blockWorkerClient = new RetryHandlingBlockWorkerClient(
        mLocalAlluxioClusterResource.get().getWorkerAddress(), mExecutorService,
        1L /* fake session id */)) {
      isConnected = true;
    } catch (IOException e) {
      isConnected = false;
    }
    Assert.assertTrue(isConnected);
  }

  /**
   * Tests Alluxio Worker client authentication, with empty client principal.
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

    boolean isConnected;
    try (BlockWorkerClient blockWorkerClient = new RetryHandlingBlockWorkerClient(
        mLocalAlluxioClusterResource.get().getWorkerAddress(), mExecutorService,
        1L /* fake session id */)) {
      isConnected = true;
    } catch (IOException e) {
      isConnected = false;
    }
    Assert.assertTrue(isConnected);
  }

  /**
   * Tests Alluxio Worker client authentication, with empty client keytab file.
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

    boolean isConnected;
    try (BlockWorkerClient blockWorkerClient = new RetryHandlingBlockWorkerClient(
        mLocalAlluxioClusterResource.get().getWorkerAddress(), mExecutorService,
        1L /* fake session id */)) {
      isConnected = true;
    } catch (IOException e) {
      isConnected = false;
    }
    Assert.assertFalse(isConnected);
  }

  /**
   * Tests Alluxio Worker client authentication, with wrong client keytab file.
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

    boolean isConnected;
    try (BlockWorkerClient blockWorkerClient = new RetryHandlingBlockWorkerClient(
        mLocalAlluxioClusterResource.get().getWorkerAddress(), mExecutorService,
        1L /* fake session id */)) {
      isConnected = true;
    } catch (IOException e) {
      isConnected = false;
    }
    Assert.assertFalse(isConnected);
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
   * Tests Alluxio Worker client connects or disconnects to the Worker.
   */
  private void authenticationOperationTest() throws Exception {
    boolean isConnected;
    try (BlockWorkerClient blockWorkerClient = new RetryHandlingBlockWorkerClient(
        mLocalAlluxioClusterResource.get().getWorkerAddress(), mExecutorService,
        1L /* fake session id */)) {
      isConnected = true;
    } catch (IOException e) {
      isConnected = false;
    }
    Assert.assertTrue(isConnected);
  }

  private void clearLoginUser() throws Exception {
    LoginUserTestUtils.resetLoginUser();
  }

  private void cleanUpTicketCache() {
    // Cleanup Kerberos ticket cache.
    try {
      ShellUtils.execCommand(new String[]{"kdestroy"});
    } catch (IOException e) {
      // Ignore "kdestroy" shell results.
    }
  }
}

