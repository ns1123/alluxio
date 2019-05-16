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
import alluxio.security.User;
import alluxio.security.authentication.AuthType;
import alluxio.security.minikdc.MiniKdc;
import alluxio.security.user.HadoopDelegationTokenUserState;
import alluxio.security.user.HadoopKerberosUserState;
import alluxio.security.user.UserState;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.util.CommonUtils;

import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.PrivilegedExceptionAction;

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
    // Add the Hadoop user state factories.
    UserState.FACTORIES.add(0, new HadoopKerberosUserState.Factory());
    UserState.FACTORIES.add(0, new HadoopDelegationTokenUserState.Factory());

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
  }

  @Test
  public void kerberosLoginUserTest() throws Exception {
    ServerConfiguration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.KERBEROS.getAuthName());
    ServerConfiguration.set(PropertyKey.SECURITY_KERBEROS_CLIENT_PRINCIPAL, sFooPrincipal);
    ServerConfiguration.set(PropertyKey.SECURITY_KERBEROS_CLIENT_KEYTAB_FILE, sFooKeytab.getPath());
    ServerConfiguration.set(PropertyKey.SECURITY_KERBEROS_SERVER_PRINCIPAL, sFooPrincipal);
    ServerConfiguration.set(PropertyKey.SECURITY_KERBEROS_SERVER_KEYTAB_FILE, sFooKeytab.getPath());

    UserState s = UserState.Factory
        .create(ServerConfiguration.global(), new Subject(), CommonUtils.ProcessType.CLIENT);
    User loginUser = s.getUser();

    Assert.assertNotNull(loginUser);
    Assert.assertEquals("foo", loginUser.getName());
    Assert.assertEquals("[foo/host@EXAMPLE.COM]",
        loginUser.getSubject().getPrincipals(KerberosPrincipal.class).toString());
  }

  @Test
  public void kerberosLoginUserWithHadoopTest() throws Exception {
    ServerConfiguration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.KERBEROS.getAuthName());
    org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
    SecurityUtil.setAuthenticationMethod(
        UserGroupInformation.AuthenticationMethod.KERBEROS, hadoopConf);
    UserGroupInformation.setConfiguration(hadoopConf);
    UserGroupInformation.loginUserFromKeytab(sFooPrincipal, sFooKeytab.getPath());

    Subject subject =
        UserGroupInformation.getLoginUser().doAs((PrivilegedExceptionAction<Subject>) () -> {
          AccessControlContext context = AccessController.getContext();
          return Subject.getSubject(context);
        });
    Assert.assertNotNull(subject);

    UserState userState = UserState.Factory
        .create(ServerConfiguration.global(), subject, CommonUtils.ProcessType.CLIENT);
    User loginUser = userState.getUser();

    Assert.assertNotNull(loginUser);
    Assert.assertEquals("foo", loginUser.getName());
    Assert.assertEquals("[foo/host@EXAMPLE.COM]",
        userState.getSubject().getPrincipals(KerberosPrincipal.class).toString());
  }

  @Test
  public void kerberosLoginUserWithInvalidKeytabTest() throws Exception {
    ServerConfiguration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.KERBEROS.getAuthName());
    ServerConfiguration.set(PropertyKey.SECURITY_KERBEROS_CLIENT_PRINCIPAL, sFooPrincipal);
    ServerConfiguration.set(PropertyKey.SECURITY_KERBEROS_CLIENT_KEYTAB_FILE,
        sFooKeytab.getPath() + ".invalid");
    UserState s = UserState.Factory
        .create(ServerConfiguration.global(), new Subject(), CommonUtils.ProcessType.CLIENT);
    mThrown.expect(UnauthenticatedException.class);
    s.getUser();
  }

  @Test
  public void kerberosLoginUserWithNonexistingPrincipalTest() throws Exception {
    String nonexistPrincipal = "nonexist/host@EXAMPLE.COM";
    ServerConfiguration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.KERBEROS.getAuthName());
    ServerConfiguration.set(PropertyKey.SECURITY_KERBEROS_CLIENT_PRINCIPAL, nonexistPrincipal);
    ServerConfiguration.set(PropertyKey.SECURITY_KERBEROS_CLIENT_KEYTAB_FILE, sFooKeytab.getPath());
    UserState s = UserState.Factory
        .create(ServerConfiguration.global(), new Subject(), CommonUtils.ProcessType.CLIENT);
    mThrown.expect(UnauthenticatedException.class);
    s.getUser();
  }

  @Test
  public void kerberosLoginUserWithMissingConstantsTest() throws Exception {
    ServerConfiguration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.KERBEROS.getAuthName());

    // Login should fail without principal or keytab file present.
    UserState s = UserState.Factory
        .create(ServerConfiguration.global(), new Subject(), CommonUtils.ProcessType.CLIENT);
    mThrown.expect(UnauthenticatedException.class);
    s.getUser();
  }

  @Test
  public void simpleGetClientTest() throws Exception {
    ServerConfiguration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.SIMPLE.getAuthName());
    ServerConfiguration.set(PropertyKey.SECURITY_LOGIN_USERNAME, "foo");

    UserState userState = UserState.Factory
        .create(ServerConfiguration.global(), new Subject(), CommonUtils.ProcessType.CLIENT);
    User loginUser = userState.getUser();

    Assert.assertNotNull(loginUser);
    Assert.assertEquals("foo", loginUser.getName());
  }

  @Test
  public void kerberosGetClientTest() throws Exception {
    ServerConfiguration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.KERBEROS.getAuthName());
    ServerConfiguration.set(PropertyKey.SECURITY_KERBEROS_CLIENT_PRINCIPAL, sFooPrincipal);
    ServerConfiguration.set(PropertyKey.SECURITY_KERBEROS_CLIENT_KEYTAB_FILE, sFooKeytab.getPath());

    UserState userState = UserState.Factory
        .create(ServerConfiguration.global(), new Subject(), CommonUtils.ProcessType.CLIENT);
    User loginUser = userState.getUser();

    Assert.assertNotNull(loginUser);
    Assert.assertEquals("foo", loginUser.getName());
    Assert.assertEquals("[foo/host@EXAMPLE.COM]",
        loginUser.getSubject().getPrincipals(KerberosPrincipal.class).toString());
  }

  @Test
  public void simpleGetServerTest() throws Exception {
    ServerConfiguration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.SIMPLE.getAuthName());
    ServerConfiguration.set(PropertyKey.SECURITY_LOGIN_USERNAME, "bar");

    UserState userState = UserState.Factory
        .create(ServerConfiguration.global(), new Subject(), CommonUtils.ProcessType.MASTER);
    User loginUser = userState.getUser();

    Assert.assertNotNull(loginUser);
    Assert.assertEquals("bar", loginUser.getName());
  }

  @Test
  public void kerberosGetClientWithWrongConfigTest() throws Exception {
    ServerConfiguration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.KERBEROS.getAuthName());
    ServerConfiguration.set(PropertyKey.SECURITY_KERBEROS_CLIENT_PRINCIPAL, sFooPrincipal);
    ServerConfiguration.set(PropertyKey.SECURITY_KERBEROS_CLIENT_KEYTAB_FILE, sBarKeytab.getPath());

    UserState userState = UserState.Factory
        .create(ServerConfiguration.global(), new Subject(), CommonUtils.ProcessType.CLIENT);
    mThrown.expect(UnauthenticatedException.class);
    userState.getUser();
  }

  @Test
  public void kerberosGetServerTest() throws Exception {
    ServerConfiguration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.KERBEROS.getAuthName());
    ServerConfiguration.set(PropertyKey.SECURITY_KERBEROS_SERVER_PRINCIPAL, sBarPrincipal);
    ServerConfiguration.set(PropertyKey.SECURITY_KERBEROS_SERVER_KEYTAB_FILE, sBarKeytab.getPath());

    UserState userState = UserState.Factory
        .create(ServerConfiguration.global(), new Subject(), CommonUtils.ProcessType.MASTER);
    User loginUser = userState.getUser();

    Assert.assertNotNull(loginUser);
    Assert.assertEquals("bar", loginUser.getName());
    Assert.assertEquals("[bar/host@EXAMPLE.COM]",
        userState.getSubject().getPrincipals(KerberosPrincipal.class).toString());
  }

  @Test
  public void kerberosGetServerWithWrongConfigTest() throws Exception {
    ServerConfiguration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.KERBEROS.getAuthName());
    ServerConfiguration.set(PropertyKey.SECURITY_KERBEROS_SERVER_PRINCIPAL, sBarPrincipal);
    ServerConfiguration.set(PropertyKey.SECURITY_KERBEROS_SERVER_KEYTAB_FILE, sFooKeytab.getPath());

    mThrown.expect(UnauthenticatedException.class);
    UserState userState = UserState.Factory
        .create(ServerConfiguration.global(), new Subject(), CommonUtils.ProcessType.MASTER);
    userState.getUser();
  }

  @Test
  public void kerberosGetClientLoginSubjectTest() throws Exception {
    ServerConfiguration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.KERBEROS.getAuthName());
    ServerConfiguration.set(PropertyKey.SECURITY_KERBEROS_CLIENT_PRINCIPAL, sFooPrincipal);
    ServerConfiguration.set(PropertyKey.SECURITY_KERBEROS_CLIENT_KEYTAB_FILE, sFooKeytab.getPath());

    UserState userState = UserState.Factory
        .create(ServerConfiguration.global(), new Subject(), CommonUtils.ProcessType.CLIENT);
    Subject subject = userState.getSubject();

    Assert.assertNotNull(subject);
    Assert.assertEquals("[foo/host@EXAMPLE.COM]",
        subject.getPrincipals(KerberosPrincipal.class).toString());
  }

  @Test
  public void kerberosGetServerLoginSubjectTest() throws Exception {
    ServerConfiguration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.KERBEROS.getAuthName());
    ServerConfiguration.set(PropertyKey.SECURITY_KERBEROS_SERVER_PRINCIPAL, sBarPrincipal);
    ServerConfiguration.set(PropertyKey.SECURITY_KERBEROS_SERVER_KEYTAB_FILE, sBarKeytab.getPath());

    UserState userState = UserState.Factory
        .create(ServerConfiguration.global(), new Subject(), CommonUtils.ProcessType.MASTER);
    Subject subject = userState.getSubject();

    Assert.assertNotNull(subject);
    Assert.assertEquals("[bar/host@EXAMPLE.COM]",
        subject.getPrincipals(KerberosPrincipal.class).toString());
  }
}
