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

package alluxio.server.auth;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.FileSystemMasterClient;
import alluxio.exception.status.UnauthenticatedException;
import alluxio.exception.status.UnavailableException;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.GetStatusPOptions;
import alluxio.hadoop.AlluxioDelegationTokenIdentifier;
import alluxio.hadoop.FileSystem;
import alluxio.hadoop.HadoopKerberosLoginProvider;
import alluxio.master.LocalAlluxioCluster;
import alluxio.master.MasterClientConfig;
import alluxio.security.LoginUser;
import alluxio.security.LoginUserTestUtils;
import alluxio.security.authentication.AuthType;
import alluxio.security.minikdc.MiniKdc;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.util.network.NetworkAddressUtils;

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
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
import java.net.URI;
import java.net.URLClassLoader;
import java.security.PrivilegedExceptionAction;

/**
 * Tests RPC authentication between master and its client, in Kerberos mode.
 */
@Ignore
// TODO(ggezer) EE-SEC reactivate after gRPC kerberos.
// TODO(bin): improve the way to set and isolate MasterContext/WorkerContext across test cases
public final class MasterClientKerberosIntegrationTest extends BaseIntegrationTest {
  private static final String HOSTNAME = NetworkAddressUtils.getLocalHostName();
  private static final String UNIFIED_INSTANCE = "instance";

  private static MiniKdc sKdc;
  private static File sWorkDir;

  private static String sServerPrincipal;
  private static File sServerKeytab;

  @ClassRule
  public static final TemporaryFolder FOLDER = new TemporaryFolder();

  @ClassRule
  public static LocalAlluxioClusterResource sLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder().setStartCluster(false).build();

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

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

    sLocalAlluxioClusterResource.addProperties(ImmutableMap.<PropertyKey, Object>builder()
        .put(PropertyKey.MASTER_HOSTNAME, HOSTNAME)
        .put(PropertyKey.WORKER_HOSTNAME, HOSTNAME)
        .put(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.KERBEROS.getAuthName())
        .put(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, "true")
        .put(PropertyKey.SECURITY_KERBEROS_CLIENT_PRINCIPAL, sServerPrincipal)
        .put(PropertyKey.SECURITY_KERBEROS_CLIENT_KEYTAB_FILE, sServerKeytab.getPath())
        .put(PropertyKey.SECURITY_KERBEROS_SERVER_PRINCIPAL, sServerPrincipal)
        .put(PropertyKey.SECURITY_KERBEROS_SERVER_KEYTAB_FILE, sServerKeytab.getPath())
        .put(PropertyKey.SECURITY_KERBEROS_SERVICE_NAME, "alluxio")
        .put(PropertyKey.SECURITY_KERBEROS_UNIFIED_INSTANCE_NAME, UNIFIED_INSTANCE)
        .build());
    sLocalAlluxioClusterResource.start();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    if (sKdc != null) {
      sKdc.stop();
    }
  }

  @Before
  public void before() throws Exception {
    LoginUserTestUtils.resetLoginUser();
  }

  @After
  public void after() throws Exception {
    LoginUserTestUtils.resetLoginUser();
  }

  /**
   * Tests Alluxio client and Master authentication, in Kerberos mode.
   */
  @Test
  public void kerberosAuthenticationOpenCloseTest() throws Exception {
    Configuration.set(PropertyKey.SECURITY_KERBEROS_CLIENT_PRINCIPAL, sServerPrincipal);
    Configuration.set(PropertyKey.SECURITY_KERBEROS_CLIENT_KEYTAB_FILE, sServerKeytab.getPath());

    String filename = "/kerberos-file1";
    FileSystemMasterClient masterClient =
          FileSystemMasterClient.Factory.create(MasterClientConfig.defaults());
    Assert.assertFalse(masterClient.isConnected());
    masterClient.connect();
    Assert.assertTrue(masterClient.isConnected());
    masterClient.createFile(new AlluxioURI(filename), CreateFilePOptions.getDefaultInstance());
    Assert.assertNotNull(
        masterClient.getStatus(new AlluxioURI(filename), GetStatusPOptions.getDefaultInstance()));
    masterClient.disconnect();
    masterClient.close();
  }

  /**
   * Tests Kerberos authentication, with an isolated class loader.
   */
  @Test
  public void kerberosAuthenticationIsolatedClassLoader() throws Exception {
    Configuration.set(PropertyKey.SECURITY_KERBEROS_CLIENT_PRINCIPAL, sServerPrincipal);
    Configuration.set(PropertyKey.SECURITY_KERBEROS_CLIENT_KEYTAB_FILE, sServerKeytab.getPath());

    FileSystemMasterClient masterClient =
          FileSystemMasterClient.Factory.create(MasterClientConfig.defaults());
    Assert.assertFalse(masterClient.isConnected());

    // Get the current context class loader to retrieve the classpath URLs.
    ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
    Assert.assertTrue(contextClassLoader instanceof URLClassLoader);

    // Set the context class loader to an isolated class loader.
    ClassLoader isolatedClassLoader =
        new URLClassLoader(((URLClassLoader) contextClassLoader).getURLs(), null);
    Thread.currentThread().setContextClassLoader(isolatedClassLoader);
    try {
      masterClient.connect();
    } finally {
      Thread.currentThread().setContextClassLoader(contextClassLoader);
    }
    Assert.assertTrue(masterClient.isConnected());
  }

  /**
   * Tests Alluxio client and Master authentication, with invalid client principal.
   */
  @Test
  public void kerberosAuthenticationWithWrongPrincipalTest() throws Exception {
    // Invalid client principal.
    Configuration.set(PropertyKey.SECURITY_KERBEROS_CLIENT_PRINCIPAL, "invalid");
    Configuration.set(PropertyKey.SECURITY_KERBEROS_CLIENT_KEYTAB_FILE, sServerKeytab.getPath());

    boolean isConnected;
    try {
      FileSystemMasterClient masterClient =
          FileSystemMasterClient.Factory.create(MasterClientConfig.defaults());
      masterClient.connect();
      isConnected = masterClient.isConnected();
    } catch (UnauthenticatedException e) {
      isConnected = false;
    }
    Assert.assertFalse(isConnected);
  }

  /**
   * Tests Alluxio client and Master authentication, with wrong client keytab file.
   */
  @Test
  public void kerberosAuthenticationWithWrongKeytabTest() throws Exception {
    Configuration.set(PropertyKey.SECURITY_KERBEROS_CLIENT_PRINCIPAL, sServerPrincipal);
    // Wrong keytab file which does not contain the actual client principal credentials.
    Configuration.set(PropertyKey.SECURITY_KERBEROS_CLIENT_KEYTAB_FILE,
        sServerKeytab.getPath() + "invalidsuffix");

    boolean isConnected;
    try {
      FileSystemMasterClient masterClient =
          FileSystemMasterClient.Factory.create(MasterClientConfig.defaults());
      masterClient.connect();
      isConnected = masterClient.isConnected();
    } catch (UnauthenticatedException e) {
      isConnected = false;
    }
    Assert.assertFalse(isConnected);
  }

  /**
   * Tests Alluxio client and Master authentication, with valid delegation token.
   */
  @Test
  public void kerberosAuthenticationWithDelegationTokenTest() throws Exception {
    Configuration.set(PropertyKey.SECURITY_KERBEROS_CLIENT_PRINCIPAL, sServerPrincipal);
    Configuration.set(PropertyKey.SECURITY_KERBEROS_CLIENT_KEYTAB_FILE, sServerKeytab.getPath());
    UserGroupInformation tokenUGI = UserGroupInformation.createRemoteUser("alluxio");
    UserGroupInformation.setLoginUser(tokenUGI);
    Credentials creds = new Credentials();
    boolean isConnected = true;
    try {
      alluxio.client.file.FileSystem fs = sLocalAlluxioClusterResource.get().getClient();
      org.apache.hadoop.fs.FileSystem hdfs = new FileSystem(fs);
      org.apache.hadoop.conf.Configuration hdfsConf = new org.apache.hadoop.conf.Configuration();
      hdfs.initialize(new URI(sLocalAlluxioClusterResource.get().getMasterURI()), hdfsConf);
      // obtains delegation token
      hdfs.addDelegationTokens("yarn", creds);

      sLocalAlluxioClusterResource.get().getLocalAlluxioMaster().clearClients();
      LoginUser.reset();
      tokenUGI.addCredentials(creds);

      // accesses master using delegation token
      Configuration.unset(PropertyKey.SECURITY_KERBEROS_CLIENT_PRINCIPAL);
      Configuration.unset(PropertyKey.SECURITY_KERBEROS_CLIENT_KEYTAB_FILE);
      LoginUser.setExternalLoginProvider(new HadoopKerberosLoginProvider());

      // gets a new client
      sLocalAlluxioClusterResource.get().getLocalAlluxioMaster().clearClients();
      FileSystemContext.clearCache();
      fs = sLocalAlluxioClusterResource.get().getClient();
      FileSystem newHdfs = new FileSystem(fs);

      tokenUGI.doAs((PrivilegedExceptionAction<? extends Object>) () -> {
        newHdfs.initialize(new URI(sLocalAlluxioClusterResource.get().getMasterURI()), hdfsConf);
        Path rootPath = new Path("/");
        FileStatus rootStat = newHdfs.getFileStatus(rootPath);
        Assert.assertEquals(rootStat.getPath().getName(), rootPath.getName());
        return null;
      });
    } catch (UnauthenticatedException e) {
      isConnected = false;
    }
    Assert.assertTrue(isConnected);
  }

  /**
   * Tests Alluxio client and Master authentication, with invalid delegation token.
   */
  @Test
  public void kerberosAuthenticationWithWrongDelegationTokenTest() throws Exception {
    Configuration.set(PropertyKey.SECURITY_KERBEROS_CLIENT_PRINCIPAL, sServerPrincipal);
    Configuration.set(PropertyKey.SECURITY_KERBEROS_CLIENT_KEYTAB_FILE, sServerKeytab.getPath());
    UserGroupInformation tokenUGI = UserGroupInformation.createRemoteUser("alluxio");
    UserGroupInformation.setLoginUser(tokenUGI);
    Credentials creds = new Credentials();
    boolean isConnected = true;
    try {
      alluxio.client.file.FileSystem fs = sLocalAlluxioClusterResource.get().getClient();
      org.apache.hadoop.fs.FileSystem hdfs = new FileSystem(fs);
      org.apache.hadoop.conf.Configuration hdfsConf = new org.apache.hadoop.conf.Configuration();
      hdfs.initialize(new URI(sLocalAlluxioClusterResource.get().getMasterURI()), hdfsConf);
      // obtains delegation token
      hdfs.addDelegationTokens("yarn", creds);
      Assert.assertTrue(!creds.getAllTokens().isEmpty());

      // creates a delegation token with invalid password
      Credentials invalidCreds = new Credentials();
      Token<? extends TokenIdentifier> token = creds.getAllTokens().iterator().next();
      invalidCreds.addToken(token.getService(),
          new Token<AlluxioDelegationTokenIdentifier>(token.getIdentifier(), "foo".getBytes(),
              token.getKind(), token.getService()));
      LoginUser.reset();
      tokenUGI.addCredentials(invalidCreds);

      // accesses master using delegation token
      Configuration.unset(PropertyKey.SECURITY_KERBEROS_CLIENT_PRINCIPAL);
      Configuration.unset(PropertyKey.SECURITY_KERBEROS_CLIENT_KEYTAB_FILE);
      LoginUser.setExternalLoginProvider(new HadoopKerberosLoginProvider());

      // gets a new client
      sLocalAlluxioClusterResource.get().getLocalAlluxioMaster().clearClients();
      FileSystemContext.clearCache();
      fs = sLocalAlluxioClusterResource.get().getClient();
      FileSystem newHdfs = new FileSystem(fs);

      tokenUGI.doAs((PrivilegedExceptionAction<? extends Object>) () -> {
        newHdfs.initialize(new URI(sLocalAlluxioClusterResource.get().getMasterURI()), hdfsConf);
        Path rootPath = new Path("/");
        newHdfs.getFileStatus(rootPath);
        return null;
      });
    } catch (UnavailableException e) {
      isConnected = false;
    }
    Assert.assertFalse(isConnected);
  }

  /**
   * Tests renewal of delegation token.
   */
  @Test
  public void delegationTokenRenewalTest() throws Exception {
    Configuration.set(PropertyKey.SECURITY_KERBEROS_CLIENT_PRINCIPAL, sServerPrincipal);
    Configuration.set(PropertyKey.SECURITY_KERBEROS_CLIENT_KEYTAB_FILE, sServerKeytab.getPath());
    UserGroupInformation tokenUGI = UserGroupInformation.createRemoteUser("alluxio");
    UserGroupInformation.setLoginUser(tokenUGI);
    Credentials creds = new Credentials();
    boolean isConnected = true;
    try {
      alluxio.client.file.FileSystem fs = sLocalAlluxioClusterResource.get().getClient();
      org.apache.hadoop.fs.FileSystem hdfs = new FileSystem(fs);
      org.apache.hadoop.conf.Configuration hdfsConf = new org.apache.hadoop.conf.Configuration();
      hdfs.initialize(new URI(sLocalAlluxioClusterResource.get().getMasterURI()), hdfsConf);
      // obtains delegation token
      Token<?>[] tokens = hdfs.addDelegationTokens("alluxio", creds);
      Assert.assertEquals(1, tokens.length);
      Assert.assertEquals(
          AlluxioDelegationTokenIdentifier.ALLUXIO_DELEGATION_KIND,
          tokens[0].getKind());

      long expireTime1 = tokens[0].renew(hdfsConf);
      Thread.sleep(200L);
      long expireTime2 = tokens[0].renew(hdfsConf);

      // verifies token expiration time has updated.
      Assert.assertTrue(expireTime2 > expireTime1);

      sLocalAlluxioClusterResource.get().getLocalAlluxioMaster().clearClients();
      LoginUser.reset();
      tokenUGI.addCredentials(creds);

      // accesses master using delegation token
      Configuration.unset(PropertyKey.SECURITY_KERBEROS_CLIENT_PRINCIPAL);
      Configuration.unset(PropertyKey.SECURITY_KERBEROS_CLIENT_KEYTAB_FILE);
      LoginUser.setExternalLoginProvider(new HadoopKerberosLoginProvider());

      // gets a new client
      sLocalAlluxioClusterResource.get().getLocalAlluxioMaster().clearClients();
      FileSystemContext.clearCache();
      fs = sLocalAlluxioClusterResource.get().getClient();
      FileSystem newHdfs = new FileSystem(fs);

      tokenUGI.doAs((PrivilegedExceptionAction<? extends Object>) () -> {
        newHdfs.initialize(new URI(sLocalAlluxioClusterResource.get().getMasterURI()), hdfsConf);
        Path rootPath = new Path("/");
        FileStatus rootStat = newHdfs.getFileStatus(rootPath);
        Assert.assertEquals(rootStat.getPath().getName(), rootPath.getName());
        return null;
      });
    } catch (UnauthenticatedException | UnavailableException e) {
      isConnected = false;
    }
    Assert.assertTrue(isConnected);
  }

  /**
   * Tests renewal of delegation token.
   */
  @Test
  public void delegationTokenCancellationTest() throws Exception {
    Configuration.set(PropertyKey.SECURITY_KERBEROS_CLIENT_PRINCIPAL, sServerPrincipal);
    Configuration.set(PropertyKey.SECURITY_KERBEROS_CLIENT_KEYTAB_FILE, sServerKeytab.getPath());
    UserGroupInformation tokenUGI = UserGroupInformation.createRemoteUser("alluxio");
    UserGroupInformation.setLoginUser(tokenUGI);
    Credentials creds = new Credentials();
    boolean isConnected = true;
    try {
      alluxio.client.file.FileSystem fs = sLocalAlluxioClusterResource.get().getClient();
      org.apache.hadoop.fs.FileSystem hdfs = new FileSystem(fs);
      org.apache.hadoop.conf.Configuration hdfsConf = new org.apache.hadoop.conf.Configuration();
      hdfs.initialize(new URI(sLocalAlluxioClusterResource.get().getMasterURI()), hdfsConf);
      // obtains delegation token
      Token<?>[] tokens = hdfs.addDelegationTokens("alluxio", creds);
      Assert.assertEquals(1, tokens.length);
      Assert.assertEquals(
          AlluxioDelegationTokenIdentifier.ALLUXIO_DELEGATION_KIND,
          tokens[0].getKind());

      sLocalAlluxioClusterResource.get().getLocalAlluxioMaster().clearClients();
      LoginUser.reset();
      tokenUGI.addCredentials(creds);

      // accesses master using delegation token
      Configuration.unset(PropertyKey.SECURITY_KERBEROS_CLIENT_PRINCIPAL);
      Configuration.unset(PropertyKey.SECURITY_KERBEROS_CLIENT_KEYTAB_FILE);
      LoginUser.setExternalLoginProvider(new HadoopKerberosLoginProvider());

      // gets a new client
      sLocalAlluxioClusterResource.get().getLocalAlluxioMaster().clearClients();
      FileSystemContext.clearCache();
      fs = sLocalAlluxioClusterResource.get().getClient();
      FileSystem newHdfs = new FileSystem(fs);

      tokenUGI.doAs((PrivilegedExceptionAction<? extends Object>) () -> {
        newHdfs.initialize(new URI(sLocalAlluxioClusterResource.get().getMasterURI()), hdfsConf);
        Path rootPath = new Path("/");
        FileStatus rootStat = newHdfs.getFileStatus(rootPath);
        Assert.assertEquals(rootStat.getPath().getName(), rootPath.getName());

        tokens[0].cancel(hdfsConf);
        return null;
      });
    } catch (UnauthenticatedException | UnavailableException e) {
      isConnected = false;
    }
    Assert.assertTrue(isConnected);

    try {
      sLocalAlluxioClusterResource.get().getLocalAlluxioMaster().clearClients();
      FileSystemContext.clearCache();
      alluxio.client.file.FileSystem fs = sLocalAlluxioClusterResource.get().getClient();
      org.apache.hadoop.conf.Configuration hdfsConf = new org.apache.hadoop.conf.Configuration();
      FileSystem newHdfs = new FileSystem(fs);

      // accesses master using cancelled delegation token
      tokenUGI.doAs((PrivilegedExceptionAction<? extends Object>) () -> {
        newHdfs.initialize(new URI(sLocalAlluxioClusterResource.get().getMasterURI()), hdfsConf);
        Path rootPath = new Path("/");
        FileStatus rootStat = newHdfs.getFileStatus(rootPath);
        Assert.assertEquals(rootStat.getPath().getName(), rootPath.getName());
        return null;
      });
    } catch (UnauthenticatedException | UnavailableException e) {
      isConnected = false;
    }
    Assert.assertFalse(isConnected);
  }

  @Test
  public void journalGetDelegationToken() throws Exception {
    Configuration.set(PropertyKey.SECURITY_KERBEROS_CLIENT_PRINCIPAL, sServerPrincipal);
    Configuration.set(PropertyKey.SECURITY_KERBEROS_CLIENT_KEYTAB_FILE, sServerKeytab.getPath());
    UserGroupInformation tokenUGI = UserGroupInformation.createRemoteUser("alluxio");
    UserGroupInformation.setLoginUser(tokenUGI);
    Credentials creds = new Credentials();
    boolean isConnected = true;
    try {
      alluxio.client.file.FileSystem fs = sLocalAlluxioClusterResource.get().getClient();
      org.apache.hadoop.fs.FileSystem hdfs = new FileSystem(fs);
      org.apache.hadoop.conf.Configuration hdfsConf = new org.apache.hadoop.conf.Configuration();
      hdfs.initialize(new URI(sLocalAlluxioClusterResource.get().getMasterURI()), hdfsConf);
      // obtains delegation token
      hdfs.addDelegationTokens("yarn", creds);

      LoginUser.reset();
      tokenUGI.addCredentials(creds);

      // restart masters
      LocalAlluxioCluster cluster = sLocalAlluxioClusterResource.get();
      cluster.stopMasters();
      cluster.startMasters();

      // accesses master using delegation token
      Configuration.unset(PropertyKey.SECURITY_KERBEROS_CLIENT_PRINCIPAL);
      Configuration.unset(PropertyKey.SECURITY_KERBEROS_CLIENT_KEYTAB_FILE);
      LoginUser.setExternalLoginProvider(new HadoopKerberosLoginProvider());

      // gets a new client
      sLocalAlluxioClusterResource.get().getLocalAlluxioMaster().clearClients();
      FileSystemContext.clearCache();
      fs = sLocalAlluxioClusterResource.get().getClient();
      FileSystem newHdfs = new FileSystem(fs);

      tokenUGI.doAs((PrivilegedExceptionAction<? extends Object>) () -> {
        newHdfs.initialize(new URI(sLocalAlluxioClusterResource.get().getMasterURI()), hdfsConf);
        Path rootPath = new Path("/");
        FileStatus rootStat = newHdfs.getFileStatus(rootPath);
        Assert.assertEquals(rootStat.getPath().getName(), rootPath.getName());
        return null;
      });
    } catch (UnauthenticatedException e) {
      isConnected = false;
    }
    Assert.assertTrue(isConnected);
  }

  @Test
  public void journalRenewDelegationToken() throws Exception {
    Configuration.set(PropertyKey.SECURITY_KERBEROS_CLIENT_PRINCIPAL, sServerPrincipal);
    Configuration.set(PropertyKey.SECURITY_KERBEROS_CLIENT_KEYTAB_FILE, sServerKeytab.getPath());
    UserGroupInformation tokenUGI = UserGroupInformation.createRemoteUser("alluxio");
    UserGroupInformation.setLoginUser(tokenUGI);
    Credentials creds = new Credentials();
    boolean isConnected = true;
    try {
      alluxio.client.file.FileSystem fs = sLocalAlluxioClusterResource.get().getClient();
      org.apache.hadoop.fs.FileSystem hdfs = new FileSystem(fs);
      org.apache.hadoop.conf.Configuration hdfsConf = new org.apache.hadoop.conf.Configuration();
      hdfs.initialize(new URI(sLocalAlluxioClusterResource.get().getMasterURI()), hdfsConf);
      // obtains delegation token
      Token<?>[] tokens = hdfs.addDelegationTokens("alluxio", creds);
      Assert.assertEquals(1, tokens.length);
      Assert.assertEquals(
          AlluxioDelegationTokenIdentifier.ALLUXIO_DELEGATION_KIND,
          tokens[0].getKind());

      tokens[0].renew(hdfsConf);

      LoginUser.reset();
      tokenUGI.addCredentials(creds);

      // restart masters
      LocalAlluxioCluster cluster = sLocalAlluxioClusterResource.get();
      cluster.stopMasters();
      cluster.startMasters();

      // accesses master using delegation token
      Configuration.unset(PropertyKey.SECURITY_KERBEROS_CLIENT_PRINCIPAL);
      Configuration.unset(PropertyKey.SECURITY_KERBEROS_CLIENT_KEYTAB_FILE);
      LoginUser.setExternalLoginProvider(new HadoopKerberosLoginProvider());

      // gets a new client
      sLocalAlluxioClusterResource.get().getLocalAlluxioMaster().clearClients();
      FileSystemContext.clearCache();
      fs = sLocalAlluxioClusterResource.get().getClient();
      FileSystem newHdfs = new FileSystem(fs);

      tokenUGI.doAs((PrivilegedExceptionAction<? extends Object>) () -> {
        newHdfs.initialize(new URI(sLocalAlluxioClusterResource.get().getMasterURI()), hdfsConf);
        Path rootPath = new Path("/");
        FileStatus rootStat = newHdfs.getFileStatus(rootPath);
        Assert.assertEquals(rootStat.getPath().getName(), rootPath.getName());
        return null;
      });
    } catch (UnauthenticatedException | UnavailableException e) {
      isConnected = false;
    }
    Assert.assertTrue(isConnected);
  }

  @Test
  public void journalCancelDelegationToken() throws Exception {
    Configuration.set(PropertyKey.SECURITY_KERBEROS_CLIENT_PRINCIPAL, sServerPrincipal);
    Configuration.set(PropertyKey.SECURITY_KERBEROS_CLIENT_KEYTAB_FILE, sServerKeytab.getPath());
    UserGroupInformation tokenUGI = UserGroupInformation.createRemoteUser("alluxio");
    UserGroupInformation.setLoginUser(tokenUGI);
    Credentials creds = new Credentials();
    boolean isConnected = true;
    try {
      alluxio.client.file.FileSystem fs = sLocalAlluxioClusterResource.get().getClient();
      org.apache.hadoop.fs.FileSystem hdfs = new FileSystem(fs);
      org.apache.hadoop.conf.Configuration hdfsConf = new org.apache.hadoop.conf.Configuration();
      hdfs.initialize(new URI(sLocalAlluxioClusterResource.get().getMasterURI()), hdfsConf);
      // obtains delegation token
      Token<?>[] tokens = hdfs.addDelegationTokens("alluxio", creds);
      Assert.assertEquals(1, tokens.length);
      Assert.assertEquals(
          AlluxioDelegationTokenIdentifier.ALLUXIO_DELEGATION_KIND,
          tokens[0].getKind());

      tokens[0].cancel(hdfsConf);

      LoginUser.reset();
      tokenUGI.addCredentials(creds);

      // restart masters
      LocalAlluxioCluster cluster = sLocalAlluxioClusterResource.get();
      cluster.stopMasters();
      cluster.startMasters();

      // accesses master using delegation token
      Configuration.unset(PropertyKey.SECURITY_KERBEROS_CLIENT_PRINCIPAL);
      Configuration.unset(PropertyKey.SECURITY_KERBEROS_CLIENT_KEYTAB_FILE);
      LoginUser.setExternalLoginProvider(new HadoopKerberosLoginProvider());

      // gets a new client
      sLocalAlluxioClusterResource.get().getLocalAlluxioMaster().clearClients();
      FileSystemContext.clearCache();
      fs = sLocalAlluxioClusterResource.get().getClient();
      FileSystem newHdfs = new FileSystem(fs);

      tokenUGI.doAs((PrivilegedExceptionAction<? extends Object>) () -> {
        newHdfs.initialize(new URI(sLocalAlluxioClusterResource.get().getMasterURI()), hdfsConf);
        Path rootPath = new Path("/");
        FileStatus rootStat = newHdfs.getFileStatus(rootPath);
        Assert.assertEquals(rootStat.getPath().getName(), rootPath.getName());
        return null;
      });
    } catch (UnauthenticatedException | UnavailableException e) {
      isConnected = false;
    }
    Assert.assertFalse(isConnected);
  }
}
