/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.security;

import alluxio.Constants;
import alluxio.LocalAlluxioClusterResource;
import alluxio.client.block.BlockWorkerClient;
import alluxio.security.minikdc.MiniKdc;
import alluxio.worker.ClientMetrics;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Tests RPC authentication between worker and its client, in Kerberos mode.
 */
// TODO(bin): improve the way to set and isolate MasterContext/WorkerContext across test cases
public final class BlockWorkerClientKerberosIntegrationTest {
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
  private ExecutorService mExecutorService;

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

    mExecutorService = Executors.newFixedThreadPool(2);
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
    mExecutorService.shutdownNow();
  }

  /**
   * Tests Alluxio Worker client authentication in Kerberos mode.
   */
  @Test
  @LocalAlluxioClusterResource.Config(startCluster = false)
  public void kerberosAuthenticationOpenCloseTest() throws Exception {
    mLocalAlluxioClusterResource.addConfParams(
        new String[] {
            Constants.SECURITY_AUTHENTICATION_TYPE, "KERBEROS",
            Constants.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, "true",
            Constants.SECURITY_KERBEROS_CLIENT_PRINCIPAL, mClientPrincipal,
            Constants.SECURITY_KERBEROS_CLIENT_KEYTAB_FILE, mClientKeytab.getPath(),
            Constants.SECURITY_KERBEROS_SERVER_PRINCIPAL, mServerPrincipal,
            Constants.SECURITY_KERBEROS_SERVER_KEYTAB_FILE, mServerKeytab.getPath() }
    );
    mLocalAlluxioClusterResource.start();

    authenticationOperationTest();
  }

  /**
   * Tests Alluxio Worker client connects or disconnects to the Worker.
   */
  private void authenticationOperationTest() throws Exception {
    BlockWorkerClient blockWorkerClient = new BlockWorkerClient(
        mLocalAlluxioClusterResource.get().getWorkerAddress(),
        mExecutorService, mLocalAlluxioClusterResource.get().getWorkerConf(),
        1 /* fake session id */, true, new ClientMetrics());

    Assert.assertFalse(blockWorkerClient.isConnected());
    blockWorkerClient.connect();
    Assert.assertTrue(blockWorkerClient.isConnected());

    blockWorkerClient.close();
  }

  private void clearLoginUser() throws Exception {
    LoginUserTestUtils.resetLoginUser();
  }
}

