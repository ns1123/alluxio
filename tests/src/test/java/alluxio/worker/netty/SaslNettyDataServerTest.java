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

package alluxio.worker.netty;

import alluxio.BaseIntegrationTest;
import alluxio.Configuration;
import alluxio.ConfigurationRule;
import alluxio.ConfigurationTestUtils;
import alluxio.PropertyKey;
import alluxio.client.netty.NettyClient;
import alluxio.netty.NettyAttributes;
import alluxio.security.LoginUserTestUtils;
import alluxio.security.authentication.AuthType;
import alluxio.security.minikdc.MiniKdc;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.worker.WorkerProcess;
import alluxio.worker.block.BlockWorker;

import com.google.common.collect.ImmutableMap;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.io.File;
import java.net.InetSocketAddress;

/**
 * Tests for NettyDataServer with Kerberos authentication enabled.
 */
public final class SaslNettyDataServerTest extends BaseIntegrationTest {
  private NettyDataServer mNettyDataServer;
  private BlockWorker mBlockWorker;

  private static MiniKdc sKdc;
  private static File sWorkDir;
  private static String sHost;

  private static String sServerPrincipal;
  private static File sServerKeytab;

  @ClassRule
  public static final TemporaryFolder FOLDER = new TemporaryFolder();

  @Rule
  public ConfigurationRule mRule = new ConfigurationRule(ImmutableMap.of(
      PropertyKey.WORKER_NETWORK_NETTY_SHUTDOWN_QUIET_PERIOD, "0"));

  @BeforeClass
  public static void beforeClass() throws Exception {
    sWorkDir = FOLDER.getRoot();
    sKdc = new MiniKdc(MiniKdc.createConf(), sWorkDir);
    sKdc.start();

    sHost = NetworkAddressUtils.getLocalHostName();
    String realm = sKdc.getRealm();

    sServerPrincipal = "alluxio/" + sHost + "@" + realm;
    sServerKeytab = new File(sWorkDir, "alluxio.keytab");
    // Create a principal in miniKDC, and generate the keytab file for it.
    sKdc.createPrincipal(sServerKeytab, "alluxio/" + sHost);
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
    Configuration.set(PropertyKey.TEST_MODE, "true");
    Configuration.set(PropertyKey.MASTER_HOSTNAME, sHost);
    Configuration.set(PropertyKey.WORKER_HOSTNAME, sHost);
    Configuration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.KERBEROS.getAuthName());
    Configuration.set(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, "true");
    Configuration.set(PropertyKey.SECURITY_KERBEROS_SERVER_PRINCIPAL, sServerPrincipal);
    Configuration.set(PropertyKey.SECURITY_KERBEROS_SERVER_KEYTAB_FILE, sServerKeytab.getPath());
    Configuration.set(PropertyKey.SECURITY_KERBEROS_CLIENT_PRINCIPAL, sServerPrincipal);
    Configuration.set(PropertyKey.SECURITY_KERBEROS_CLIENT_KEYTAB_FILE, sServerKeytab.getPath());
    Configuration.set(PropertyKey.SECURITY_KERBEROS_SERVICE_NAME, "alluxio");

    mBlockWorker = Mockito.mock(BlockWorker.class);
    WorkerProcess workerProcess = Mockito.mock(WorkerProcess.class);
    Mockito.when(workerProcess.getWorker(BlockWorker.class)).thenReturn(mBlockWorker);

    mNettyDataServer = new NettyDataServer(
        new InetSocketAddress(NetworkAddressUtils.getLocalHostName(), 0), workerProcess);
  }

  @After
  public void after() throws Exception {
    mNettyDataServer.close();
    ConfigurationTestUtils.resetConfiguration();
  }

  @Test (timeout = 60000L)
  public void authentication() throws Exception {
    InetSocketAddress address = (InetSocketAddress) mNettyDataServer.getBindAddress();
    Bootstrap clientBootstrap = NettyClient.createClientBootstrap(address);
    clientBootstrap.attr(NettyAttributes.HOSTNAME_KEY, address.getHostName());
    ChannelFuture f = clientBootstrap.connect(address).sync();
    Channel channel = f.channel();
    // Waits for the channel authentication complete.
    NettyClient.waitForChannelReady(channel);
    channel.close().sync();
  }
}
