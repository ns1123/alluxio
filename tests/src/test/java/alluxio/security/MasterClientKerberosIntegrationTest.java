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
import alluxio.LocalAlluxioClusterResource;
import alluxio.PropertyKey;
import alluxio.client.file.FileSystemMasterClient;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.exception.status.UnauthenticatedException;
import alluxio.security.authentication.AuthType;
import alluxio.security.minikdc.MiniKdc;
import alluxio.util.network.NetworkAddressUtils;

import com.google.common.collect.ImmutableMap;
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
import java.net.URLClassLoader;

/**
 * Tests RPC authentication between master and its client, in Kerberos mode.
 */
// TODO(bin): improve the way to set and isolate MasterContext/WorkerContext across test cases
public final class MasterClientKerberosIntegrationTest {
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

    String host = NetworkAddressUtils.getLocalHostName();
    String realm = sKdc.getRealm();

    sServerPrincipal = "alluxio/" + host + "@" + realm;
    sServerKeytab = new File(sWorkDir, "alluxio.keytab");
    // Create a principal in miniKDC, and generate the keytab file for it.
    sKdc.createPrincipal(sServerKeytab, "alluxio/" + host);

    sLocalAlluxioClusterResource.addProperties(ImmutableMap.<PropertyKey, Object>builder()
        .put(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.KERBEROS.getAuthName())
        .put(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, "true")
        .put(PropertyKey.SECURITY_KERBEROS_CLIENT_PRINCIPAL, sServerPrincipal)
        .put(PropertyKey.SECURITY_KERBEROS_CLIENT_KEYTAB_FILE, sServerKeytab.getPath())
        .put(PropertyKey.SECURITY_KERBEROS_SERVER_PRINCIPAL, sServerPrincipal)
        .put(PropertyKey.SECURITY_KERBEROS_SERVER_KEYTAB_FILE, sServerKeytab.getPath())
        .put(PropertyKey.SECURITY_KERBEROS_SERVICE_NAME, "alluxio")
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
    FileSystemMasterClient masterClient = FileSystemMasterClient.Factory
        .create(sLocalAlluxioClusterResource.get().getLocalAlluxioMaster().getAddress());
    Assert.assertFalse(masterClient.isConnected());
    masterClient.connect();
    Assert.assertTrue(masterClient.isConnected());
    masterClient.createFile(new AlluxioURI(filename), CreateFileOptions.defaults());
    Assert.assertNotNull(masterClient.getStatus(new AlluxioURI(filename)));
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

    FileSystemMasterClient masterClient = FileSystemMasterClient.Factory
        .create(sLocalAlluxioClusterResource.get().getLocalAlluxioMaster().getAddress());
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
      FileSystemMasterClient masterClient = FileSystemMasterClient.Factory
          .create(sLocalAlluxioClusterResource.get().getLocalAlluxioMaster().getAddress());
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
      FileSystemMasterClient masterClient = FileSystemMasterClient.Factory
          .create(sLocalAlluxioClusterResource.get().getLocalAlluxioMaster().getAddress());
      masterClient.connect();
      isConnected = masterClient.isConnected();
    } catch (UnauthenticatedException e) {
      isConnected = false;
    }
    Assert.assertFalse(isConnected);
  }
}
