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

package alluxio.security.authentication;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.security.LoginUser;
import alluxio.security.minikdc.MiniKdc;
import alluxio.util.network.NetworkAddressUtils;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;

/**
 * Tests for {@link ThriftServerProvider}.
 */
public final class ThriftServerProviderTest {
  private Configuration mConfiguration;
  private ThriftProtocolProvider mProtocol;
  private ThriftServerProvider mServer;

  private InetSocketAddress mServerAddress;
  private TServerSocket mServerTSocket;
  private TransportProvider mTransportProvider;

  private MiniKdc mKdc;
  private File mWorkDir;

  private String mServerServiceName;
  private String mClientPrincipal;
  private File mClientKeytab;
  private String mServerPrincipal;
  private File mServerKeytab;

  /**
   * Temporary folder for miniKDC keytab files.
   */
  @Rule
  public final TemporaryFolder mFolder = new TemporaryFolder();

  /**
   * Sets up the miniKDC and the server before running a test.
   */
  @Before
  public void before() throws Exception {
    Field field = LoginUser.class.getDeclaredField("sLoginUser");
    field.setAccessible(true);
    field.set(null, null);

    mConfiguration = new Configuration();
    String localhost = NetworkAddressUtils.getLocalHostName(new Configuration());
    mServerTSocket = new TServerSocket(new InetSocketAddress(localhost, 0));
    int port = NetworkAddressUtils.getThriftPort(mServerTSocket);
    mServerAddress = new InetSocketAddress(localhost, port);

    mWorkDir = mFolder.getRoot();
    mKdc = new MiniKdc(MiniKdc.createConf(), mWorkDir);
    mKdc.start();

    mClientPrincipal = "foo/host@EXAMPLE.COM";
    mClientKeytab = new File(mWorkDir, "foo.keytab");
    // Create a principal in miniKDC, and generate the keytab file for it.
    mKdc.createPrincipal(mClientKeytab, "foo/host");

    mServerServiceName = "host";
    mServerPrincipal = "server/host@EXAMPLE.COM";
    mServerKeytab = new File(mWorkDir, "server.keytab");
    // Create a principal in miniKDC, and generate the keytab file for it.
    mKdc.createPrincipal(mServerKeytab, "server/host");
  }

  /**
   * Stops the miniKDC.
   */
  @After
  public void after() {
    if (mKdc != null) {
      mKdc.stop();
    }
  }

  /**
   * Tests {@link ThriftServerProvider} in {@link AuthType#NOSASL} mode.
   */
  @Test
  public void nosaslProtocolProviderTest() throws Exception {
    mConfiguration.set(Constants.SECURITY_AUTHENTICATION_TYPE, AuthType.NOSASL.getAuthName());
    mTransportProvider = TransportProvider.Factory.create(mConfiguration);

    testServerAndProtocolProvider();
  }

  /**
   * Tests {@link ThriftServerProvider} in {@link AuthType#SIMPLE} mode.
   */
  @Test
  public void simpleProtocolProviderTest() throws Exception {
    mConfiguration.set(Constants.SECURITY_AUTHENTICATION_TYPE, AuthType.SIMPLE.getAuthName());
    mTransportProvider = TransportProvider.Factory.create(mConfiguration);

    testServerAndProtocolProvider();
  }

  /**
   * Tests {@link ThriftServerProvider} in {@link AuthType#KERBEROS} mode.
   */
  @Test
  public void kerberosProtocolProviderTest() throws Exception {
    mConfiguration.set(Constants.SECURITY_AUTHENTICATION_TYPE, AuthType.KERBEROS.getAuthName());
    mConfiguration.set(Constants.SECURITY_KERBEROS_SERVER_PRINCIPAL, mServerPrincipal);
    mConfiguration.set(Constants.SECURITY_KERBEROS_SERVER_KEYTAB_FILE, mServerKeytab.getPath());
    mConfiguration.set(Constants.SECURITY_KERBEROS_CLIENT_PRINCIPAL, mClientPrincipal);
    mConfiguration.set(Constants.SECURITY_KERBEROS_CLIENT_KEYTAB_FILE, mClientKeytab.getPath());
    mTransportProvider = TransportProvider.Factory.create(mConfiguration);

    testServerAndProtocolProvider();
  }

  private void testServerAndProtocolProvider() throws TTransportException, IOException {
    TTransportFactory tTransportFactory = mTransportProvider.getServerTransportFactory();
    // Create Server via ThriftServerProvider.
    mServer = new ThriftServerProvider(mConfiguration,
        new TThreadPoolServer.Args(mServerTSocket).maxWorkerThreads(2).minWorkerThreads(1)
            .processor(null).transportFactory(tTransportFactory)
            .protocolFactory(new TBinaryProtocol.Factory(true, true)));

    // Start the server in a new thread.
    Thread serverThread = new Thread(new Runnable() {
      @Override
      public void run() {
        mServer.serve();
      }
    });
    serverThread.start();

    mProtocol = new ThriftProtocolProvider(
        mConfiguration, new TBinaryProtocol(mTransportProvider.getClientTransport(mServerAddress)),
        mServerServiceName);
    mProtocol.openTransport();
    mProtocol.closeTransport();

    mServer.stop();
  }
}
