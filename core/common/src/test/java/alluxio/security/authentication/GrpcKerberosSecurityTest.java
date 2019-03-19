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

package alluxio.security.authentication;

import alluxio.conf.InstancedConfiguration;
import alluxio.grpc.GrpcChannelBuilder;
import alluxio.grpc.GrpcServer;
import alluxio.grpc.GrpcServerBuilder;
import alluxio.conf.PropertyKey;
import alluxio.security.LoginUserTestUtils;
import alluxio.security.minikdc.MiniKdc;
import alluxio.util.ConfigurationUtils;
import alluxio.util.ShellUtils;
import alluxio.util.network.NetworkAddressUtils;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Unit test for {@link alluxio.grpc.GrpcChannelBuilder} and {@link alluxio.grpc.GrpcServerBuilder}
 * for Kerberos scheme.
 */
public class GrpcKerberosSecurityTest {
  private static final String UNIFIED_INSTANCE = "instance";
  private static MiniKdc sKdc;
  private static File sWorkDir;
  private static String sServerPrincipal;
  private static File sServerKeytab;
  private InstancedConfiguration mConfiguration;

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

  @Before
  public void before() {
    mConfiguration = new InstancedConfiguration(ConfigurationUtils.defaults());
    String hostName = NetworkAddressUtils.getLocalHostName(
        (int) mConfiguration.getMs(PropertyKey.NETWORK_HOST_RESOLUTION_TIMEOUT_MS));

    LoginUserTestUtils.resetLoginUser();
    // Set server-side and client-side Kerberos configuration for Netty authentication.
    mConfiguration.set(PropertyKey.TEST_MODE, "true");
    mConfiguration.set(PropertyKey.MASTER_HOSTNAME, hostName);
    mConfiguration.set(PropertyKey.WORKER_HOSTNAME, hostName);
    mConfiguration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.KERBEROS.getAuthName());
    mConfiguration.set(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, "true");
    mConfiguration.set(PropertyKey.SECURITY_KERBEROS_SERVER_PRINCIPAL, sServerPrincipal);
    mConfiguration.set(PropertyKey.SECURITY_KERBEROS_SERVER_KEYTAB_FILE, sServerKeytab.getPath());
    mConfiguration.set(PropertyKey.SECURITY_KERBEROS_SERVICE_NAME, "alluxio");
    mConfiguration.set(PropertyKey.SECURITY_KERBEROS_UNIFIED_INSTANCE_NAME, UNIFIED_INSTANCE);
  }

  @After
  public void after() throws Exception {
    cleanUpTicketCache();
  }

  @Test
  public void validKerberosCredential() throws Exception {
    mConfiguration.set(PropertyKey.SECURITY_KERBEROS_CLIENT_PRINCIPAL, sServerPrincipal);
    mConfiguration.set(PropertyKey.SECURITY_KERBEROS_CLIENT_KEYTAB_FILE, sServerKeytab.getPath());
    establishKerberoizedChannel();
  }

  @Test
  public void invalidClientPrincipal() throws Exception {
    mConfiguration.set(PropertyKey.SECURITY_KERBEROS_CLIENT_PRINCIPAL, "invalid");
    mConfiguration.set(PropertyKey.SECURITY_KERBEROS_CLIENT_KEYTAB_FILE, sServerKeytab.getPath());

    try {
      establishKerberoizedChannel();
      Assert.fail("Expected createChannel() to fail with an invalid client principal.");
    } catch (Exception e) {
      // Expected
    }
  }

  @Test
  public void invalidClientKeytab() throws Exception {
    mConfiguration.set(PropertyKey.SECURITY_KERBEROS_CLIENT_PRINCIPAL, sServerPrincipal);
    mConfiguration.set(PropertyKey.SECURITY_KERBEROS_CLIENT_KEYTAB_FILE,
        sServerKeytab.getPath().concat("invalidsuffix"));

    try {
      establishKerberoizedChannel();
      Assert.fail("Expected createChannel() to fail with an invalid client keytab file.");
    } catch (Exception e) {
      // Expected
    }
  }

  private void establishKerberoizedChannel() throws Exception {
    mConfiguration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.KERBEROS);

    GrpcServer server = createServer(AuthType.KERBEROS);
    server.start();

    GrpcChannelBuilder channelBuilder =
        GrpcChannelBuilder.newBuilder(getServerConnectAddress(server), mConfiguration);
    channelBuilder.build();
    server.shutdown();
  }

  private InetSocketAddress getServerConnectAddress(GrpcServer server) {
    return new InetSocketAddress(
        NetworkAddressUtils.getLocalHostName(
            (int) mConfiguration.getMs(PropertyKey.NETWORK_HOST_RESOLUTION_TIMEOUT_MS)),
        server.getBindPort());
  }

  private GrpcServer createServer(AuthType authType) {
    mConfiguration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, authType.name());
    InetSocketAddress bindAddress = new InetSocketAddress(NetworkAddressUtils.getLocalHostName(
        (int) mConfiguration.getMs(PropertyKey.NETWORK_HOST_RESOLUTION_TIMEOUT_MS)), 0);
    GrpcServerBuilder serverBuilder =
        GrpcServerBuilder.forAddress("localhost", bindAddress, mConfiguration);
    return serverBuilder.build();
  }

  private void cleanUpTicketCache() {
    // Cleanup Kerberos ticket cache.
    try {
      ShellUtils.execCommand(new String[] {"kdestroy"});
    } catch (IOException e) {
      // Ignore "kdestroy" shell results.
    }
  }
}
