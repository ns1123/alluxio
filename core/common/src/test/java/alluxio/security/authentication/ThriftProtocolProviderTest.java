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
import alluxio.security.login.LoginModuleConfiguration;
import alluxio.security.minikdc.MiniKdc;
import alluxio.util.network.NetworkAddressUtils;

import com.google.common.collect.Sets;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;
import java.util.HashSet;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.login.LoginContext;

/**
 * Tests for {@link ThriftProtocolProvider}.
 */
public final class ThriftProtocolProviderTest {
  private Configuration mConfiguration;
  private ThriftProtocolProvider mProtocol;
  private InetSocketAddress mServerAddress;
  private TServerSocket mServerTSocket;
  private TransportProvider mTransportProvider;
  private TThreadPoolServer mServer;

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
   * Stops the miniKDC and the serving server.
   */
  @After
  public void after() {
    if (mKdc != null) {
      mKdc.stop();
    }
    mServer.stop();
  }

  /**
   * Tests {@link ThriftProtocolProvider#openTransport()} in {@link AuthType#NOSASL} mode.
   */
  @Test
  public void nosaslProtocolProviderTest() throws Exception {
    mConfiguration.set(Constants.SECURITY_AUTHENTICATION_TYPE, AuthType.NOSASL.getAuthName());
    mTransportProvider = TransportProvider.Factory.create(mConfiguration);

    // start server
    startServerThread();

    mProtocol = new ThriftProtocolProvider(
        mConfiguration, new TBinaryProtocol(mTransportProvider.getClientTransport(mServerAddress)),
        mServerServiceName);
    mProtocol.openTransport();
    mProtocol.closeTransport();
  }

  /**
   * Tests {@link ThriftProtocolProvider#openTransport()} in {@link AuthType#SIMPLE} mode.
   */
  @Test
  public void simpleProtocolProviderTest() throws Exception {
    mConfiguration.set(Constants.SECURITY_AUTHENTICATION_TYPE, AuthType.SIMPLE.getAuthName());
    mTransportProvider = TransportProvider.Factory.create(mConfiguration);

    // start server
    startServerThread();

    mProtocol = new ThriftProtocolProvider(
        mConfiguration, new TBinaryProtocol(mTransportProvider.getClientTransport(mServerAddress)),
        mServerServiceName);
    mProtocol.openTransport();
    mProtocol.closeTransport();
  }

  /**
   * Tests {@link ThriftProtocolProvider#openTransport()} in {@link AuthType#KERBEROS} mode.
   */
  @Test
  public void kerberosProtocolProviderTest() throws Exception {
    mConfiguration.set(Constants.SECURITY_AUTHENTICATION_TYPE, AuthType.KERBEROS.getAuthName());
    mConfiguration.set(Constants.SECURITY_KERBEROS_SERVER_PRINCIPAL, mServerPrincipal);
    mConfiguration.set(Constants.SECURITY_KERBEROS_SERVER_KEYTAB_FILE, mServerKeytab.getPath());
    mConfiguration.set(Constants.SECURITY_KERBEROS_CLIENT_PRINCIPAL, mClientPrincipal);
    mConfiguration.set(Constants.SECURITY_KERBEROS_CLIENT_KEYTAB_FILE, mClientKeytab.getPath());
    mTransportProvider = TransportProvider.Factory.create(mConfiguration);

    final Subject serverSubject = loginKerberosPrinciple(mServerPrincipal, mServerKeytab.getPath());
    // Start Kerberos server running as server principal.
    Subject.doAs(serverSubject, new PrivilegedExceptionAction<Void>() {
      public Void run() throws Exception {
        startKerberosServerThreadWithConf();
        return null;
      }
    });

    mProtocol = new ThriftProtocolProvider(
        mConfiguration, new TBinaryProtocol(mTransportProvider.getClientTransport(mServerAddress)),
        mServerServiceName);
    mProtocol.openTransport();
    mProtocol.closeTransport();
  }

  private Subject loginKerberosPrinciple(String principal,
                                         String keytabFilePath) throws Exception {
    // Login principal with Kerberos.
    final Subject subject = new Subject(false, Sets.newHashSet(
        new KerberosPrincipal(principal)),
        new HashSet<Object>(), new HashSet<Object>());
    // Create Kerberos login configuration with principal and keytab file.
    LoginModuleConfiguration loginConf = new LoginModuleConfiguration(principal, keytabFilePath);

    // Kerberos login.
    LoginContext loginContext = new LoginContext("kerberos", subject, null, loginConf);
    loginContext.login();

    Assert.assertFalse(subject.getPrivateCredentials().isEmpty());

    return subject;
  }

  private void startKerberosServerThreadWithConf() throws Exception {
     // create args and use them to build a Thrift TServer
    TTransportFactory tTransportFactory = mTransportProvider.getServerTransportFactory();
    startServerWithTransportFactory(tTransportFactory);
  }

  private void startServerWithTransportFactory(TTransportFactory factory) throws Exception {
    mServer = new TThreadPoolServer(
        new TThreadPoolServer.Args(mServerTSocket).maxWorkerThreads(2).minWorkerThreads(1)
            .processor(null).transportFactory(factory)
            .protocolFactory(new TBinaryProtocol.Factory(true, true)));

    // Start the server in a new thread.
    Thread serverThread = new Thread(new Runnable() {
      @Override
      public void run() {
        mServer.serve();
      }
    });

    serverThread.start();

    // Ensure server is running, and break if it does not start serving in 2 seconds.
    int count = 40;
    while (!mServer.isServing() && serverThread.isAlive()) {
      if (count <= 0) {
        throw new RuntimeException("TThreadPoolServer does not start serving");
      }
      Thread.sleep(50);
      count--;
    }
  }

  private void startServerThread() throws Exception {
    // Create args and use them to build a Thrift TServer.
    TTransportFactory tTransportFactory = mTransportProvider.getServerTransportFactory();
    startServerWithTransportFactory(tTransportFactory);
  }
}
