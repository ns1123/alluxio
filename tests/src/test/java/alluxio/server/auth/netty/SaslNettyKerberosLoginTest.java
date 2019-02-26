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

package alluxio.server.auth.netty;

import alluxio.ConfigurationRule;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.security.LoginUserTestUtils;
import alluxio.security.authentication.AuthType;
import alluxio.security.minikdc.MiniKdc;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.util.ShellUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.worker.WorkerProcess;
import alluxio.worker.block.BlockWorker;

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
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;

/**
 * Tests for Netty authentication with different Kerberos credential combinations.
 */
// TODO(ggezer) EE-SEC implement for gRPC kerberos.
@Ignore
public final class SaslNettyKerberosLoginTest extends BaseIntegrationTest {
  private static final String HOSTNAME = NetworkAddressUtils
      .getLocalHostName(ServerConfiguration.getInt(PropertyKey.NETWORK_HOST_RESOLUTION_TIMEOUT_MS));
  private static final String UNIFIED_INSTANCE = "instance";

  //private NettyDataServer mNettyDataServer;
  private BlockWorker mBlockWorker;

  private static MiniKdc sKdc;
  private static File sWorkDir;

  private static String sServerPrincipal;
  private static File sServerKeytab;

  @ClassRule
  public static final TemporaryFolder FOLDER = new TemporaryFolder();

  @Rule
  public ConfigurationRule mRule = new ConfigurationRule(ImmutableMap.of(
      PropertyKey.WORKER_NETWORK_NETTY_SHUTDOWN_QUIET_PERIOD, "0sec"),
      ServerConfiguration.global());

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

  @Before
  public void before() {
    LoginUserTestUtils.resetLoginUser();
    // Set server-side and client-side Kerberos configuration for Netty authentication.
    ServerConfiguration.set(PropertyKey.TEST_MODE, "true");
    ServerConfiguration.set(PropertyKey.MASTER_HOSTNAME, HOSTNAME);
    ServerConfiguration.set(PropertyKey.WORKER_HOSTNAME, HOSTNAME);
    ServerConfiguration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.KERBEROS.getAuthName());
    ServerConfiguration.set(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, "true");
    ServerConfiguration.set(PropertyKey.SECURITY_KERBEROS_SERVER_PRINCIPAL, sServerPrincipal);
    ServerConfiguration.set(PropertyKey.SECURITY_KERBEROS_SERVER_KEYTAB_FILE, sServerKeytab.getPath());
    ServerConfiguration.set(PropertyKey.SECURITY_KERBEROS_SERVICE_NAME, "alluxio");
    ServerConfiguration.set(PropertyKey.SECURITY_KERBEROS_UNIFIED_INSTANCE_NAME, UNIFIED_INSTANCE);

    // Note: mock workers here to bypass thrift authentication and directly test netty data path.
    // Otherwise invalid Kerberos login would first fail on the thrift protocol.
    mBlockWorker = Mockito.mock(BlockWorker.class);
    WorkerProcess workerProcess = Mockito.mock(WorkerProcess.class);
    Mockito.when(workerProcess.getWorker(BlockWorker.class)).thenReturn(mBlockWorker);

    //mNettyDataServer = new NettyDataServer(
    //    new InetSocketAddress(NetworkAddressUtils.getLocalHostName(), 0), workerProcess);
  }

  @After
  public void after() throws Exception {
    cleanUpTicketCache();
    //mNettyDataServer.close();
    ServerConfiguration.reset();
  }

  @Test
  public void validKerberosCredential() throws Exception {
    ServerConfiguration.set(PropertyKey.SECURITY_KERBEROS_CLIENT_PRINCIPAL, sServerPrincipal);
    ServerConfiguration.set(PropertyKey.SECURITY_KERBEROS_CLIENT_KEYTAB_FILE, sServerKeytab.getPath());

    createChannel();
  }

  @Test
  public void invalidClientPrincipal() throws Exception {
    ServerConfiguration.set(PropertyKey.SECURITY_KERBEROS_CLIENT_PRINCIPAL, "invalid");
    ServerConfiguration.set(PropertyKey.SECURITY_KERBEROS_CLIENT_KEYTAB_FILE, sServerKeytab.getPath());

    try {
      createChannel();
      Assert.fail("Expected createChannel() to fail with an invalid client principal.");
    } catch (Exception e) {
      // Expected
    }
  }

  @Test
  public void invalidClientKeytab() throws Exception {
    ServerConfiguration.set(PropertyKey.SECURITY_KERBEROS_CLIENT_PRINCIPAL, sServerPrincipal);
    ServerConfiguration.set(PropertyKey.SECURITY_KERBEROS_CLIENT_KEYTAB_FILE,
        sServerKeytab.getPath().concat("invalidsuffix"));

    try {
      createChannel();
      Assert.fail("Expected createChannel() to fail with an invalid client keytab file.");
    } catch (Exception e) {
      // Expected
    }
  }

  /**
   * Creates a client bootstrap and waits until the channel is ready.
   */
  private void createChannel() throws IOException, InterruptedException {
    //InetSocketAddress address = (InetSocketAddress) mNettyDataServer.getBindAddress();
    //Bootstrap clientBootstrap = NettyClient.createClientBootstrap(null, address);
    //clientBootstrap.attr(NettyAttributes.HOSTNAME_KEY, address.getHostName());
    //ChannelFuture f = clientBootstrap.connect(address).sync();
    //Channel channel = f.channel();
    //try {
    //  // Waits for the channel authentication complete.
    //  NettyUtils.waitForClientChannelReady(channel);
    //} finally {
    //  channel.close().sync();
    //}
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
