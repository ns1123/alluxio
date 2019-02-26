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

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.status.UnauthenticatedException;
import alluxio.hadoop.HadoopKerberosLoginProvider;
import alluxio.security.LoginUser;
import alluxio.security.LoginUserTestUtils;
import alluxio.security.User;
import alluxio.security.authentication.AuthType;
import alluxio.security.minikdc.MiniKdc;
import alluxio.testutils.BaseIntegrationTest;

import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.File;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;

/**
 * Unit test for the Kerberos user login.
 */
public final class KerberosLoginUserTest extends BaseIntegrationTest {
  private static MiniKdc sKdc;
  private static File sWorkDir;

  private static String sFooPrincipal;
  private static File sFooKeytab;
  private static String sBarPrincipal;
  private static File sBarKeytab;

  /**
   * The exception expected to be thrown.
   */
  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @ClassRule
  public static final TemporaryFolder FOLDER = new TemporaryFolder();

  @BeforeClass
  public static void beforeClass() throws Exception {
    sWorkDir = FOLDER.getRoot();
    sKdc = new MiniKdc(MiniKdc.createConf(), sWorkDir);
    sKdc.start();

    sFooPrincipal = "foo/host@EXAMPLE.COM";
    sFooKeytab = new File(sWorkDir, "foo.keytab");
    // Create a principal in miniKDC, and generate the keytab file for it.
    sKdc.createPrincipal(sFooKeytab, "foo/host");

    sBarPrincipal = "bar/host@EXAMPLE.COM";
    sBarKeytab = new File(sWorkDir, "bar.keytab");
    // Create a principal in miniKDC, and generate the keytab file for it.
    sKdc.createPrincipal(sBarKeytab, "bar/host");

    LoginUserTestUtils.resetLoginUser();
  }

  @AfterClass
  public static void afterClass() {
    if (sKdc != null) {
      sKdc.stop();
    }
  }

  @After
  public void after() {
    ServerConfiguration.reset();
    LoginUserTestUtils.resetLoginUser();
    LoginUser.setExternalLoginProvider(null);
  }

  /**
   * Tests the {@link LoginUser} with valid Kerberos principal and keytab.
   */
  @Ignore("This won't work post-configuration singleton removal. LoginUser changes will "
      + "remove/modify this test soon")
  @Test
  public void kerberosLoginUserTest() throws Exception {
    ServerConfiguration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.KERBEROS.getAuthName());
    ServerConfiguration.set(PropertyKey.SECURITY_KERBEROS_CLIENT_PRINCIPAL, sFooPrincipal);
    ServerConfiguration.set(PropertyKey.SECURITY_KERBEROS_CLIENT_KEYTAB_FILE, sFooKeytab.getPath());
    ServerConfiguration.set(PropertyKey.SECURITY_KERBEROS_SERVER_PRINCIPAL, sFooPrincipal);
    ServerConfiguration.set(PropertyKey.SECURITY_KERBEROS_SERVER_KEYTAB_FILE, sFooKeytab.getPath());

    User loginUser = LoginUser.get(ServerConfiguration.global());

    Assert.assertNotNull(loginUser);
    Assert.assertEquals("foo", loginUser.getName());
    Assert.assertEquals("[foo/host@EXAMPLE.COM]",
        loginUser.getSubject().getPrincipals(KerberosPrincipal.class).toString());
  }

  /**
   * Tests the {@link LoginUser} with valid Kerberos principal and keytab from Hadoop APIs.
   */
  @Test
  public void kerberosLoginUserWithHadoopTest() throws Exception {
    ServerConfiguration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.KERBEROS.getAuthName());
    LoginUser.setExternalLoginProvider(new HadoopKerberosLoginProvider());
    org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
    SecurityUtil.setAuthenticationMethod(
        UserGroupInformation.AuthenticationMethod.KERBEROS, hadoopConf);
    UserGroupInformation.setConfiguration(hadoopConf);
    UserGroupInformation.loginUserFromKeytab(sFooPrincipal, sFooKeytab.getPath());

    User loginUser = LoginUser.getClientUser(ServerConfiguration.global());

    Assert.assertNotNull(loginUser);
    Assert.assertEquals("foo", loginUser.getName());
    Assert.assertEquals("[foo/host@EXAMPLE.COM]",
        loginUser.getSubject().getPrincipals(KerberosPrincipal.class).toString());
  }

  /**
   * Tests the {@link LoginUser} with invalid keytab file.
   */
  @Test
  public void kerberosLoginUserWithInvalidKeytabTest() throws Exception {
    ServerConfiguration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.KERBEROS.getAuthName());
    ServerConfiguration.set(PropertyKey.SECURITY_KERBEROS_LOGIN_PRINCIPAL, sFooPrincipal);
    ServerConfiguration.set(PropertyKey.SECURITY_KERBEROS_LOGIN_KEYTAB_FILE,
        sFooKeytab.getPath() + ".invalid");
    mThrown.expect(UnauthenticatedException.class);
    LoginUser.get(ServerConfiguration.global());
  }

  /**
   * Tests the {@link LoginUser} with non-exsiting principal.
   */
  @Test
  public void kerberosLoginUserWithNonexistingPrincipalTest() throws Exception {
    String nonexistPrincipal = "nonexist/host@EXAMPLE.COM";
    ServerConfiguration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.KERBEROS.getAuthName());
    ServerConfiguration.set(PropertyKey.SECURITY_KERBEROS_LOGIN_PRINCIPAL, nonexistPrincipal);
    ServerConfiguration.set(PropertyKey.SECURITY_KERBEROS_LOGIN_KEYTAB_FILE, sFooKeytab.getPath());
    mThrown.expect(UnauthenticatedException.class);
    LoginUser.get(ServerConfiguration.global());
  }

  /**
   * Tests the {@link LoginUser} with missing Kerberos required constants.
   */
  @Test
  public void kerberosLoginUserWithMissingConstantsTest() throws Exception {
    ServerConfiguration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.KERBEROS.getAuthName());

    // Login should fail without principal or keytab file present.
    mThrown.expect(UnauthenticatedException.class);
    LoginUser.get(ServerConfiguration.global());
  }

  /**
   * Tests for {@link LoginUser#getClientUser(alluxio.conf.AlluxioConfiguration)} in SIMPLE auth mode.
   */
  @Test
  public void simpleGetClientTest() throws Exception {
    ServerConfiguration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.SIMPLE.getAuthName());
    ServerConfiguration.set(PropertyKey.SECURITY_LOGIN_USERNAME, "foo");

    User loginUser = LoginUser.getClientUser(ServerConfiguration.global());

    Assert.assertNotNull(loginUser);
    Assert.assertEquals("foo", loginUser.getName());
  }

  /**
   * Tests for {@link LoginUser#getClientUser(alluxio.conf.AlluxioConfiguration)}in KERBEROS auth mode.
   */
  @Ignore("This won't work post-configuration singleton removal. LoginUser changes will "
      + "remove/modify this test soon")
  @Test
  public void kerberosGetClientTest() throws Exception {
    ServerConfiguration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.KERBEROS.getAuthName());
    ServerConfiguration.set(PropertyKey.SECURITY_KERBEROS_CLIENT_PRINCIPAL, sFooPrincipal);
    ServerConfiguration.set(PropertyKey.SECURITY_KERBEROS_CLIENT_KEYTAB_FILE, sFooKeytab.getPath());

    User loginUser = LoginUser.getClientUser(ServerConfiguration.global());

    Assert.assertNotNull(loginUser);
    Assert.assertEquals("foo", loginUser.getName());
    Assert.assertEquals("[foo/host@EXAMPLE.COM]",
        loginUser.getSubject().getPrincipals(KerberosPrincipal.class).toString());
  }

  /**
   * Tests for {@link LoginUser#getServerUser(alluxio.conf.AlluxioConfiguration)} in SIMPLE auth mode.
   */
  @Test
  public void simpleGetServerTest() throws Exception {
    ServerConfiguration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.SIMPLE.getAuthName());
    ServerConfiguration.set(PropertyKey.SECURITY_LOGIN_USERNAME, "bar");

    User loginUser = LoginUser.getServerUser(ServerConfiguration.global());

    Assert.assertNotNull(loginUser);
    Assert.assertEquals("bar", loginUser.getName());
  }

  /**
   * Tests for {@link LoginUser#getClientUser(alluxio.conf.AlluxioConfiguration)}in KERBEROS with
   * wrong config.
   */
  @Test
  public void kerberosGetClientWithWrongConfigTest() throws Exception {
    ServerConfiguration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.KERBEROS.getAuthName());
    ServerConfiguration.set(PropertyKey.SECURITY_KERBEROS_CLIENT_PRINCIPAL, sFooPrincipal);
    ServerConfiguration.set(PropertyKey.SECURITY_KERBEROS_CLIENT_KEYTAB_FILE, sBarKeytab.getPath());

    mThrown.expect(UnauthenticatedException.class);
    LoginUser.getClientUser(ServerConfiguration.global());
  }

  /**
   * Tests for {@link LoginUser#getServerUser(alluxio.conf.AlluxioConfiguration)} in KERBEROS auth mode.
   */
  @Ignore("This won't work post-configuration singleton removal. LoginUser changes will "
      + "remove/modify this test soon")
  @Test
  public void kerberosGetServerTest() throws Exception {
    ServerConfiguration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.KERBEROS.getAuthName());
    ServerConfiguration.set(PropertyKey.SECURITY_KERBEROS_SERVER_PRINCIPAL, sBarPrincipal);
    ServerConfiguration.set(PropertyKey.SECURITY_KERBEROS_SERVER_KEYTAB_FILE, sBarKeytab.getPath());

    User loginUser = LoginUser.getServerUser(ServerConfiguration.global());

    Assert.assertNotNull(loginUser);
    Assert.assertEquals("bar", loginUser.getName());
    Assert.assertEquals("[bar/host@EXAMPLE.COM]",
        loginUser.getSubject().getPrincipals(KerberosPrincipal.class).toString());
  }

  /**
   * Tests for {@link LoginUser#getServerUser(alluxio.conf.AlluxioConfiguration)} in KERBEROS
   * with wrong config.
   */
  @Test
  public void kerberosGetServerWithWrongConfigTest() throws Exception {
    ServerConfiguration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.KERBEROS.getAuthName());
    ServerConfiguration.set(PropertyKey.SECURITY_KERBEROS_SERVER_PRINCIPAL, sBarPrincipal);
    ServerConfiguration.set(PropertyKey.SECURITY_KERBEROS_SERVER_KEYTAB_FILE, sFooKeytab.getPath());

    mThrown.expect(UnauthenticatedException.class);
    LoginUser.getServerUser(ServerConfiguration.global());
  }

  /**
   * Tests for {@link LoginUser#getClientLoginSubject(alluxio.conf.AlluxioConfiguration)} in KERBEROS mode.
   */
  @Ignore("This won't work post-configuration singleton removal. LoginUser changes will "
      + "remove/modify this test soon")
  @Test
  public void kerberosGetClientLoginSubjectTest() throws Exception {
    ServerConfiguration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.KERBEROS.getAuthName());
    ServerConfiguration.set(PropertyKey.SECURITY_KERBEROS_CLIENT_PRINCIPAL, sFooPrincipal);
    ServerConfiguration.set(PropertyKey.SECURITY_KERBEROS_CLIENT_KEYTAB_FILE, sFooKeytab.getPath());

    Subject subject = LoginUser.getClientLoginSubject(ServerConfiguration.global());

    Assert.assertNotNull(subject);
    Assert.assertEquals("[foo/host@EXAMPLE.COM]",
        subject.getPrincipals(KerberosPrincipal.class).toString());
  }

  /**
   * Tests for {@link LoginUser#getServerLoginSubject(alluxio.conf.AlluxioConfiguration)}.
   */
  @Ignore("This won't work post-configuration singleton removal. LoginUser changes will "
      + "remove/modify this test soon")
  @Test
  public void kerberosGetServerLoginSubjectTest() throws Exception {
    ServerConfiguration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.KERBEROS.getAuthName());
    ServerConfiguration.set(PropertyKey.SECURITY_KERBEROS_SERVER_PRINCIPAL, sBarPrincipal);
    ServerConfiguration.set(PropertyKey.SECURITY_KERBEROS_SERVER_KEYTAB_FILE, sBarKeytab.getPath());

    Subject subject = LoginUser.getServerLoginSubject(ServerConfiguration.global());

    Assert.assertNotNull(subject);
    Assert.assertEquals("[bar/host@EXAMPLE.COM]",
        subject.getPrincipals(KerberosPrincipal.class).toString());
  }

  /**
   * Tests for {@link LoginUser#getClientLoginSubject(alluxio.conf.AlluxioConfiguration)} and
   * {@link LoginUser#getServerLoginSubject(alluxio.conf.AlluxioConfiguration)} in SIMPLE mode.
   */
  @Test
  public void simpleGetLoginSubjectTest() throws Exception {
    ServerConfiguration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.SIMPLE.getAuthName());

    Subject subject = LoginUser.getClientLoginSubject(ServerConfiguration.global());
    Assert.assertNull(subject);

    subject = LoginUser.getServerLoginSubject(ServerConfiguration.global());
    Assert.assertNull(subject);
  }
}
