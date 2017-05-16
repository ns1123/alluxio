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
import alluxio.PropertyKey;
import alluxio.security.authentication.AuthType;
import alluxio.security.authentication.AuthenticatedThriftProtocol;
import alluxio.security.authentication.KerberosSaslTransportProvider;
import alluxio.security.authentication.TransportProvider;
import alluxio.security.login.LoginModuleConfiguration;
import alluxio.security.minikdc.MiniKdc;
import alluxio.util.network.NetworkAddressUtils;

import com.google.common.collect.Sets;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.net.InetSocketAddress;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.HashSet;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.login.LoginContext;

/**
 * Unit tests for methods in {@link AuthenticatedThriftProtocol} and
 * {@link KerberosSaslTransportProvider}.
 *
 * In order to test methods that return kinds of TTransport for connection in different mode, we
 * build Thrift servers and clients with specific TTransport, and let them connect.
 */
public final class TransportAndProtocolAuthenticationTest {
  private TThreadPoolServer mServer;
  private InetSocketAddress mServerAddress;
  private TServerSocket mServerTSocket;
  private TransportProvider mTransportProvider;

  private static MiniKdc sKdc;
  private static File sWorkDir;

  private static String sServerProtocol;
  private static String sServerName;
  private static String sClientPrincipal;
  private static File sClientKeytab;
  private static String sServerPrincipal;
  private static File sServerKeytab;

  /**
   * The exception expected to be thrown.
   */
  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @ClassRule
  public static final TemporaryFolder FOLDER = new TemporaryFolder();

  @BeforeClass
  public static void beforeClass() throws Exception {
    ConfigurationTestUtils.resetConfiguration();

    sWorkDir = FOLDER.getRoot();
    sKdc = new MiniKdc(MiniKdc.createConf(), sWorkDir);
    sKdc.start();

    sServerName = NetworkAddressUtils.getLocalHostName();

    sClientPrincipal = "foo/" + sServerName + "@EXAMPLE.COM";
    sClientKeytab = new File(sWorkDir, "foo.keytab");
    // Create a principal in miniKDC, and generate the keytab file for it.
    sKdc.createPrincipal(sClientKeytab, "foo/" + sServerName);

    sServerProtocol = "alluxio";
    sServerPrincipal = "alluxio/" + sServerName + "@EXAMPLE.COM";
    sServerKeytab = new File(sWorkDir, "alluxio.keytab");
    // Create a principal in miniKDC, and generate the keytab file for it.
    sKdc.createPrincipal(sServerKeytab, "alluxio/" + sServerName);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    if (sKdc != null) {
      sKdc.stop();
    }
  }

  @Before
  public void before() throws Exception {
    // Use port 0 to assign each test case an available port (possibly different)
    Configuration.set(PropertyKey.TEST_MODE, "true");
    mServerTSocket = new TServerSocket(new InetSocketAddress(sServerName, 0));
    int port = NetworkAddressUtils.getThriftPort(mServerTSocket);
    mServerAddress = new InetSocketAddress(sServerName, port);
  }

  @After
  public void after() {
    mServerTSocket.close();
    LoginUserTestUtils.resetLoginUser();
    ConfigurationTestUtils.resetConfiguration();
  }

  /**
   * Tests {@link AuthenticatedThriftProtocol} methods in {@link AuthType#KERBEROS} mode.
   */
  @Test
  public void kerberosAuthenticatedProtocolTest() throws Exception {
    Configuration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.KERBEROS.getAuthName());
    Configuration.set(PropertyKey.SECURITY_KERBEROS_SERVER_PRINCIPAL, sServerPrincipal);
    Configuration.set(PropertyKey.SECURITY_KERBEROS_SERVER_KEYTAB_FILE, sServerKeytab.getPath());
    Configuration.set(PropertyKey.SECURITY_KERBEROS_CLIENT_PRINCIPAL, sClientPrincipal);
    Configuration.set(PropertyKey.SECURITY_KERBEROS_CLIENT_KEYTAB_FILE, sClientKeytab.getPath());
    Configuration.set(PropertyKey.SECURITY_KERBEROS_SERVICE_NAME, "alluxio");
    mTransportProvider = TransportProvider.Factory.create();

    // start server
    final Subject serverSubject = loginKerberosPrinciple(sServerPrincipal, sServerKeytab.getPath());
    // start Kerberos server running as server principal.
    Subject.doAs(serverSubject, new PrivilegedExceptionAction<Void>() {
      public Void run() throws Exception {
        startServerThread();
        return null;
      }
    });

    AuthenticatedThriftProtocol protocol = new AuthenticatedThriftProtocol(
        new TBinaryProtocol(mTransportProvider.getClientTransport(mServerAddress)), sServerName);
    protocol.openTransport();
    Assert.assertTrue(protocol.getTransport().isOpen());

    protocol.closeTransport();

    mServer.stop();
  }

  /**
   * Tests {@link KerberosSaslTransportProvider#getClientTransportInternal}
   * and {@link KerberosSaslTransportProvider#getServerTransportFactoryInternal}.
   */
  @Test
  public void kerberosSaslTransportProviderInternalTest() throws Exception {
    Configuration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.KERBEROS.getAuthName());
    mTransportProvider = TransportProvider.Factory.create();

    final Subject serverSubject = loginKerberosPrinciple(sServerPrincipal, sServerKeytab.getPath());
    // start Kerberos server running as server principal.
    Subject.doAs(serverSubject, new PrivilegedExceptionAction<Void>() {
      public Void run() throws Exception {
        startKerberosServerThread(serverSubject, sServerProtocol, sServerName);
        return null;
      }
    });

    final Subject clientSubject = loginKerberosPrinciple(sClientPrincipal, sClientKeytab.getPath());
    // Get client thrift transport with Kerberos.
    final TTransport client = ((KerberosSaslTransportProvider) mTransportProvider)
        .getClientTransportInternal(
            clientSubject, sServerProtocol, sServerName, mServerAddress);

    try {
      Subject.doAs(clientSubject, new PrivilegedExceptionAction<Void>() {
        public Void run() throws TTransportException {
          client.open();
          return null;
        }
      });
    } finally {
      mServer.stop();
    }
  }

  /**
   * Tests {@link KerberosSaslTransportProvider#getClientTransport}
   * and {@link KerberosSaslTransportProvider#getServerTransportFactory}.
   */
  @Test
  public void kerberosSaslTransportProviderTest() throws Exception {
    Configuration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.KERBEROS.getAuthName());
    Configuration.set(PropertyKey.SECURITY_KERBEROS_SERVER_PRINCIPAL, sServerPrincipal);
    Configuration.set(PropertyKey.SECURITY_KERBEROS_SERVER_KEYTAB_FILE, sServerKeytab.getPath());
    Configuration.set(PropertyKey.SECURITY_KERBEROS_CLIENT_PRINCIPAL, sClientPrincipal);
    Configuration.set(PropertyKey.SECURITY_KERBEROS_CLIENT_KEYTAB_FILE, sClientKeytab.getPath());
    Configuration.set(PropertyKey.SECURITY_KERBEROS_SERVICE_NAME, "alluxio");
    mTransportProvider = TransportProvider.Factory.create();

    final Subject serverSubject = loginKerberosPrinciple(sServerPrincipal, sServerKeytab.getPath());
    // start Kerberos server running as server principal.
    Subject.doAs(serverSubject, new PrivilegedExceptionAction<Void>() {
      public Void run() throws Exception {
        startServerThread();
        return null;
      }
    });

    final Subject clientSubject = loginKerberosPrinciple(sClientPrincipal, sClientKeytab.getPath());
    // Get client thrift transport with Kerberos.
    final TTransport client = mTransportProvider.getClientTransport(mServerAddress);

    try {
      Subject.doAs(clientSubject, new PrivilegedExceptionAction<Void>() {
        public Void run() throws TTransportException {
          client.open();
          return null;
        }
      });
    } finally {
      mServer.stop();
    }
  }

  /**
   * In KERBEROS mode, tests the authentication failure if the server name is wrong.
   */
  @Test
  public void kerberosAuthenticationWithWrongServerNameTest() throws Exception {
    Configuration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.KERBEROS.getAuthName());
    mTransportProvider = TransportProvider.Factory.create();

    final Subject serverSubject = loginKerberosPrinciple(sServerPrincipal, sServerKeytab.getPath());
    // start Kerberos server running as server principal.
    Subject.doAs(serverSubject, new PrivilegedExceptionAction<Void>() {
      public Void run() throws Exception {
        startKerberosServerThread(serverSubject, sServerProtocol, "wrongservername");
        return null;
      }
    });

    final Subject clientSubject = loginKerberosPrinciple(sClientPrincipal, sClientKeytab.getPath());
    // Get client thrift transport with Kerberos.
    final TTransport client = ((KerberosSaslTransportProvider) mTransportProvider)
        .getClientTransportInternal(
            clientSubject, sServerProtocol, "wrongservername", mServerAddress);

    mThrown.expect(PrivilegedActionException.class);
    try {
      Subject.doAs(clientSubject, new PrivilegedExceptionAction<Void>() {
        public Void run() throws TTransportException {
          client.open();
          return null;
        }
      });
    } finally {
      mServer.stop();
    }
  }

  /**
   * In KERBEROS mode, tests the authentication failure if the server is not logged in via Kerberos.
   */
  @Test
  public void kerberosAuthenticationWithServerNotLoginTest() throws Exception {
    Configuration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.KERBEROS.getAuthName());
    mTransportProvider = TransportProvider.Factory.create();

    // Create serverSubject but not login.
    final Subject serverSubject = new Subject(false, Sets.newHashSet(
        new KerberosPrincipal(sServerPrincipal)),
        new HashSet<>(), new HashSet<>());

    // start Kerberos server running as server principal.
    Subject.doAs(serverSubject, new PrivilegedExceptionAction<Void>() {
      public Void run() throws Exception {
        startKerberosServerThread(serverSubject, sServerProtocol, sServerName);
        return null;
      }
    });

    final Subject clientSubject = loginKerberosPrinciple(sClientPrincipal, sClientKeytab.getPath());
    // Get client thrift transport with Kerberos.
    final TTransport client = ((KerberosSaslTransportProvider) mTransportProvider)
        .getClientTransportInternal(
            clientSubject, sServerProtocol, sServerName, mServerAddress);

    mThrown.expect(PrivilegedActionException.class);
    try {
      Subject.doAs(clientSubject, new PrivilegedExceptionAction<Void>() {
        public Void run() throws TTransportException {
          client.open();
          return null;
        }
      });
    } finally {
      mServer.stop();
    }
  }

  /**
   * In KERBEROS mode, tests the authentication failure if the server is not run as the Subject.
   */
  @Test
  public void kerberosAuthenticationWithServerNotRunAsSubjectTest() throws Exception {
    Configuration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.KERBEROS.getAuthName());
    mTransportProvider = TransportProvider.Factory.create();

    final Subject serverSubject = loginKerberosPrinciple(sServerPrincipal, sServerKeytab.getPath());
    startKerberosServerThread(serverSubject, sServerProtocol, sServerName);

    final Subject clientSubject = loginKerberosPrinciple(sClientPrincipal, sClientKeytab.getPath());
    // Get client thrift transport with Kerberos.
    final TTransport client = ((KerberosSaslTransportProvider) mTransportProvider)
        .getClientTransportInternal(
            clientSubject, sServerProtocol, sServerName, mServerAddress);

    mThrown.expect(PrivilegedActionException.class);
    try {
      Subject.doAs(clientSubject, new PrivilegedExceptionAction<Void>() {
        public Void run() throws TTransportException {
          client.open();
          return null;
        }
      });
    } finally {
      mServer.stop();
    }
  }

  /**
   * In KERBEROS mode, tests the authentication failure if the client is not logged in via Kerberos.
   */
  @Test
  public void kerberosAuthenticationWithClientNotLoginTest() throws Exception {
    Configuration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.KERBEROS.getAuthName());
    mTransportProvider = TransportProvider.Factory.create();

    final Subject serverSubject = loginKerberosPrinciple(sServerPrincipal, sServerKeytab.getPath());
    // start Kerberos server running as server principal.
    Subject.doAs(serverSubject, new PrivilegedExceptionAction<Void>() {
      public Void run() throws Exception {
        startKerberosServerThread(serverSubject, sServerProtocol, sServerName);
        return null;
      }
    });

    // Create clientSubject but not login.
    final Subject clientSubject = new Subject(false, Sets.newHashSet(
        new KerberosPrincipal(sClientPrincipal)),
        new HashSet<>(), new HashSet<>());
    // Get client thrift transport with Kerberos.
    final TTransport client = ((KerberosSaslTransportProvider) mTransportProvider)
        .getClientTransportInternal(
            clientSubject, sServerProtocol, sServerName, mServerAddress);

    mThrown.expect(PrivilegedActionException.class);
    try {
      Subject.doAs(clientSubject, new PrivilegedExceptionAction<Void>() {
        public Void run() throws TTransportException {
          client.open();
          return null;
        }
      });
    } finally {
      mServer.stop();
    }
  }

  private Subject loginKerberosPrinciple(String principal,
                                         String keytabFilePath) throws Exception {
    // Login principal with Kerberos.
    final Subject subject = new Subject(false, Sets.newHashSet(
        new KerberosPrincipal(principal)),
        new HashSet<>(), new HashSet<>());
    // Create Kerberos login configuration with principal and keytab file.
    LoginModuleConfiguration loginConf = new LoginModuleConfiguration(principal, keytabFilePath);

    // Kerberos login.
    LoginContext loginContext = new LoginContext("kerberos", subject, null, loginConf);
    loginContext.login();

    Assert.assertFalse(subject.getPrivateCredentials().isEmpty());

    return subject;
  }

  private void startKerberosServerThread(Subject subject,
                                         String protocol,
                                         String serverName) throws Exception {
    // create args and use them to build a Thrift TServer
    TTransportFactory tTransportFactory = ((KerberosSaslTransportProvider) mTransportProvider)
        .getServerTransportFactoryInternal(subject, protocol, serverName, new Runnable() {
          @Override
          public void run() {}
        });
    startServerWithTransportFactory(tTransportFactory);
  }

  private void startServerThread() throws Exception {
    // Create args and use them to build a Thrift TServer
    TTransportFactory tTransportFactory = mTransportProvider.getServerTransportFactory("test");
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
}
