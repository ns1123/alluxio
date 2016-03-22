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

package alluxio.security;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.security.authentication.AuthType;
import alluxio.security.minikdc.MiniKdc;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;

/**
 * Unit test for the Kerberos user login.
 */
public final class KerberosLoginUserTest {
  private MiniKdc mKdc;
  private File mWorkDir;

  private String mFooPrincipal;
  private File mFooKeytab;
  private String mBarPrincipal;
  private File mBarKeytab;

  /**
   * The exception expected to be thrown.
   */
  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  /**
   * Temporary folder for miniKDC keytab files.
   */
  @Rule
  public final TemporaryFolder mFolder = new TemporaryFolder();

  /**
   * Starts the miniKDC and creates the principals.
   */
  @Before
  public void before() throws Exception {
    mWorkDir = mFolder.getRoot();
    mKdc = new MiniKdc(MiniKdc.createConf(), mWorkDir);
    mKdc.start();

    mFooPrincipal = "foo/host@EXAMPLE.COM";
    mFooKeytab = new File(mWorkDir, "foo.keytab");
    // Create a principal in miniKDC, and generate the keytab file for it.
    mKdc.createPrincipal(mFooKeytab, "foo/host");

    mBarPrincipal = "bar/host@EXAMPLE.COM";
    mBarKeytab = new File(mWorkDir, "bar.keytab");
    // Create a principal in miniKDC, and generate the keytab file for it.
    mKdc.createPrincipal(mBarKeytab, "bar/host");

    Field field = LoginUser.class.getDeclaredField("sLoginUser");
    field.setAccessible(true);
    field.set(null, null);
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
   * Tests the {@link LoginUser} with valid Kerberos principal and keytab.
   */
  @Test
  public void kerberosLoginUserTest() throws Exception {
    Configuration conf = new Configuration();
    conf.set(Constants.SECURITY_AUTHENTICATION_TYPE, AuthType.KERBEROS.getAuthName());
    conf.set(Constants.SECURITY_KERBEROS_LOGIN_PRINCIPAL, mFooPrincipal);
    conf.set(Constants.SECURITY_KERBEROS_LOGIN_KEYTAB_FILE, mFooKeytab.getPath());

    User loginUser = LoginUser.get(conf);

    Assert.assertNotNull(loginUser);
    Assert.assertEquals("foo/host@EXAMPLE.COM", loginUser.getName());
    Assert.assertEquals("[foo/host@EXAMPLE.COM]",
        loginUser.getSubject().getPrincipals(KerberosPrincipal.class).toString());
  }

  /**
   * Tests the {@link LoginUser} with invalid keytab file.
   */
  @Test
  public void kerberosLoginUserWithInvalidKeytabTest() throws Exception {
    Configuration conf = new Configuration();
    conf.set(Constants.SECURITY_AUTHENTICATION_TYPE, AuthType.KERBEROS.getAuthName());
    conf.set(Constants.SECURITY_KERBEROS_LOGIN_PRINCIPAL, mFooPrincipal);
    conf.set(Constants.SECURITY_KERBEROS_LOGIN_KEYTAB_FILE, mFooKeytab.getPath() + ".invalid");
    mThrown.expect(IOException.class);
    LoginUser.get(conf);
  }

  /**
   * Tests the {@link LoginUser} with non-exsiting principal.
   */
  @Test
  public void kerberosLoginUserWithNonexistingPrincipalTest() throws Exception {
    String nonexistPrincipal = "nonexist/host@EXAMPLE.COM";
    Configuration conf = new Configuration();
    conf.set(Constants.SECURITY_AUTHENTICATION_TYPE, AuthType.KERBEROS.getAuthName());
    conf.set(Constants.SECURITY_KERBEROS_LOGIN_PRINCIPAL, nonexistPrincipal);
    conf.set(Constants.SECURITY_KERBEROS_LOGIN_KEYTAB_FILE, mFooKeytab.getPath());
    mThrown.expect(IOException.class);
    LoginUser.get(conf);
  }

  /**
   * Tests the {@link LoginUser} with missing Kerberos required constants.
   */
  @Test
  public void kerberosLoginUserWithMissingConstantsTest() throws Exception {
    Configuration conf = new Configuration();
    conf.set(Constants.SECURITY_AUTHENTICATION_TYPE, AuthType.KERBEROS.getAuthName());

    // Login should fail without principal or keytab file present.
    mThrown.expect(IOException.class);
    LoginUser.get(conf);
  }

  /**
   * Tests for {@link LoginUser#getClient(Configuration)} in SIMPLE auth mode.
   */
  @Test
  public void simpleGetClientTest() throws Exception {
    Configuration conf = new Configuration();
    conf.set(Constants.SECURITY_AUTHENTICATION_TYPE, AuthType.SIMPLE.getAuthName());
    conf.set(Constants.SECURITY_LOGIN_USERNAME, "foo");

    User loginUser = LoginUser.getClient(conf);

    Assert.assertNotNull(loginUser);
    Assert.assertEquals("foo", loginUser.getName());
  }

  /**
   * Tests for {@link LoginUser#getServer(Configuration)} in SIMPLE auth mode.
   */
  @Test
  public void simpleGetServerTest() throws Exception {
    Configuration conf = new Configuration();
    conf.set(Constants.SECURITY_AUTHENTICATION_TYPE, AuthType.SIMPLE.getAuthName());
    conf.set(Constants.SECURITY_LOGIN_USERNAME, "bar");

    User loginUser = LoginUser.getServer(conf);

    Assert.assertNotNull(loginUser);
    Assert.assertEquals("bar", loginUser.getName());
  }

  /**
   * Tests for {@link LoginUser#getClient(Configuration)} in KERBEROS auth mode.
   */
  @Test
  public void kerberosGetClientTest() throws Exception {
    Configuration conf = new Configuration();
    conf.set(Constants.SECURITY_AUTHENTICATION_TYPE, AuthType.KERBEROS.getAuthName());
    conf.set(Constants.SECURITY_KERBEROS_CLIENT_PRINCIPAL, mFooPrincipal);
    conf.set(Constants.SECURITY_KERBEROS_CLIENT_KEYTAB_FILE, mFooKeytab.getPath());

    User loginUser = LoginUser.getClient(conf);

    Assert.assertNotNull(loginUser);
    Assert.assertEquals("foo/host@EXAMPLE.COM", loginUser.getName());
    Assert.assertEquals("[foo/host@EXAMPLE.COM]",
        loginUser.getSubject().getPrincipals(KerberosPrincipal.class).toString());
  }

  /**
   * Tests for {@link LoginUser#getClient(Configuration)} in KERBEROS with wrong config.
   */
  @Test
  public void kerberosGetClientWithWrongConfigTest() throws Exception {
    Configuration conf = new Configuration();
    conf.set(Constants.SECURITY_AUTHENTICATION_TYPE, AuthType.KERBEROS.getAuthName());
    conf.set(Constants.SECURITY_KERBEROS_CLIENT_PRINCIPAL, mFooPrincipal);
    conf.set(Constants.SECURITY_KERBEROS_CLIENT_KEYTAB_FILE, mBarKeytab.getPath());

    mThrown.expect(IOException.class);
    LoginUser.getClient(conf);
  }

  /**
   * Tests for {@link LoginUser#getServer(Configuration)} in KERBEROS auth mode.
   */
  @Test
  public void kerberosGetServerTest() throws Exception {
    Configuration conf = new Configuration();
    conf.set(Constants.SECURITY_AUTHENTICATION_TYPE, AuthType.KERBEROS.getAuthName());
    conf.set(Constants.SECURITY_KERBEROS_SERVER_PRINCIPAL, mBarPrincipal);
    conf.set(Constants.SECURITY_KERBEROS_SERVER_KEYTAB_FILE, mBarKeytab.getPath());

    User loginUser = LoginUser.getServer(conf);

    Assert.assertNotNull(loginUser);
    Assert.assertEquals("bar/host@EXAMPLE.COM", loginUser.getName());
    Assert.assertEquals("[bar/host@EXAMPLE.COM]",
        loginUser.getSubject().getPrincipals(KerberosPrincipal.class).toString());
  }

  /**
   * Tests for {@link LoginUser#getServer(Configuration)} in KERBEROS with wrong config.
   */
  @Test
  public void kerberosGetServerWithWrongConfigTest() throws Exception {
    Configuration conf = new Configuration();
    conf.set(Constants.SECURITY_AUTHENTICATION_TYPE, AuthType.KERBEROS.getAuthName());
    conf.set(Constants.SECURITY_KERBEROS_SERVER_PRINCIPAL, mBarPrincipal);
    conf.set(Constants.SECURITY_KERBEROS_SERVER_KEYTAB_FILE, mFooKeytab.getPath());

    mThrown.expect(IOException.class);
    LoginUser.getServer(conf);
  }

  /**
   * Tests for {@link LoginUser#getClientLoginSubject(Configuration)} in KERBEROS mode.
   */
  @Test
  public void kerberosGetClientLoginSubjectTest() throws Exception {
    Configuration conf = new Configuration();
    conf.set(Constants.SECURITY_AUTHENTICATION_TYPE, AuthType.KERBEROS.getAuthName());
    conf.set(Constants.SECURITY_KERBEROS_CLIENT_PRINCIPAL, mFooPrincipal);
    conf.set(Constants.SECURITY_KERBEROS_CLIENT_KEYTAB_FILE, mFooKeytab.getPath());

    Subject subject = LoginUser.getClientLoginSubject(conf);

    Assert.assertNotNull(subject);
    Assert.assertEquals("[foo/host@EXAMPLE.COM]",
        subject.getPrincipals(KerberosPrincipal.class).toString());
  }

  /**
   * Tests for {@link LoginUser#getServerLoginSubject(Configuration)}.
   */
  @Test
  public void kerberosGetServerLoginSubjectTest() throws Exception {
    Configuration conf = new Configuration();
    conf.set(Constants.SECURITY_AUTHENTICATION_TYPE, AuthType.KERBEROS.getAuthName());
    conf.set(Constants.SECURITY_KERBEROS_SERVER_PRINCIPAL, mBarPrincipal);
    conf.set(Constants.SECURITY_KERBEROS_SERVER_KEYTAB_FILE, mBarKeytab.getPath());

    Subject subject = LoginUser.getServerLoginSubject(conf);

    Assert.assertNotNull(subject);
    Assert.assertEquals("[bar/host@EXAMPLE.COM]",
        subject.getPrincipals(KerberosPrincipal.class).toString());
  }

  /**
   * Tests for {@link LoginUser#getClientLoginSubject(Configuration)} and
   * {@link LoginUser#getServerLoginSubject(Configuration)} in SIMPLE mode.
   */
  @Test
  public void simpleGetLoginSubjectTest() throws Exception {
    Configuration conf = new Configuration();
    conf.set(Constants.SECURITY_AUTHENTICATION_TYPE, AuthType.SIMPLE.getAuthName());

    Subject subject = LoginUser.getClientLoginSubject(conf);
    Assert.assertNull(subject);

    subject = LoginUser.getServerLoginSubject(conf);
    Assert.assertNull(subject);
  }
}
