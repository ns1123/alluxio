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

import alluxio.ConfigurationTestUtils;
import alluxio.security.login.LoginModuleConfiguration;
import alluxio.security.minikdc.MiniKdc;

import com.google.common.collect.Sets;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.lang.reflect.Field;
import java.util.HashSet;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

/**
 * Unit test for the Kerberos login module.
 */
public final class KerberosLoginModuleTest {
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
  }

  /**
   * Stops the miniKDC.
   */
  @After
  public void after() throws Exception {
    if (mKdc != null) {
      mKdc.stop();
    }
    ConfigurationTestUtils.resetConfiguration();
    // TODO(chaomin): add logout() in LoginUser to get rid of this.
    Field field = LoginUser.class.getDeclaredField("sLoginUser");
    field.setAccessible(true);
    field.set(null, null);
  }

  /**
   * Tests the Kerberos {@link LoginModuleConfiguration}.
   */
  @Test
  public void kerberosLoginTest() throws Exception {
    Subject subject = new Subject(false, Sets.newHashSet(new KerberosPrincipal(mFooPrincipal)),
        new HashSet<Object>(), new HashSet<Object>());
    // Create Kerberos login configuration with principal and keytab file.
    LoginModuleConfiguration loginConf = new LoginModuleConfiguration(
        mFooPrincipal, mFooKeytab.getPath());

    // Kerberos login.
    LoginContext loginContext = new LoginContext("kerberos", subject, null, loginConf);
    loginContext.login();

    // Verify the login result.
    Assert.assertFalse(subject.getPrincipals(KerberosPrincipal.class).isEmpty());
    Assert.assertEquals("[foo/host@EXAMPLE.COM]",
        subject.getPrincipals(KerberosPrincipal.class).toString());

    // logout and verify the user is removed.
    loginContext.logout();
    Assert.assertTrue(subject.getPrincipals(KerberosPrincipal.class).isEmpty());

    // logout twice should be no-op.
    loginContext.logout();
    Assert.assertTrue(subject.getPrincipals(KerberosPrincipal.class).isEmpty());
  }

  /**
   * Tests the Kerberos Login with foo principal and bar keytab.
   */
  @Test
  public void kerberosLoginWithWrongPrincipalTest() throws Exception {
    Subject subject = new Subject(false, Sets.newHashSet(new KerberosPrincipal(mFooPrincipal)),
        new HashSet<Object>(), new HashSet<Object>());
    LoginModuleConfiguration loginConf = new LoginModuleConfiguration(
        mFooPrincipal, mBarKeytab.getPath());
    // Kerberos login should fail.
    LoginContext loginContext = new LoginContext("kerberos", subject, null, loginConf);
    mThrown.expect(LoginException.class);
    loginContext.login();
  }

  /**
   * Tests the Kerberos Login with invalid keytab file.
   */
  @Test
  public void kerberosLoginWithInvalidConfTest() throws Exception {
    Subject subject = new Subject(false, Sets.newHashSet(new KerberosPrincipal(mFooPrincipal)),
        new HashSet<Object>(), new HashSet<Object>());
    LoginModuleConfiguration loginConf = new LoginModuleConfiguration(
        mFooPrincipal, mFooKeytab.getPath() + ".invalid");
    // Kerberos login should fail.
    LoginContext loginContext = new LoginContext("kerberos", subject, null, loginConf);
    mThrown.expect(LoginException.class);
    loginContext.login();
  }

  /**
   * Tests the Kerberos Login with non-existing principal.
   */
  @Test
  public void kerberosLoginWithNonexistingPrincipalTest() throws Exception {
    // Case 3: Create Kerberos login configuration with non-existing principal.
    String nonexistPrincipal = "nonexist/host@EXAMPLE.COM";
    Subject subject = new Subject(false, Sets.newHashSet(new KerberosPrincipal(nonexistPrincipal)),
        new HashSet<Object>(), new HashSet<Object>());
    LoginModuleConfiguration loginConf = new LoginModuleConfiguration(
        nonexistPrincipal, mFooKeytab.getPath());
    // Kerberos login should fail.
    LoginContext loginContext = new LoginContext("kerberos", subject, null, loginConf);
    mThrown.expect(LoginException.class);
    loginContext.login();
  }

  /**
   * Tests the Kerberos Login with missing principal and keytab.
   */
  @Test
  public void kerberosLoginWithMissingConfTest() throws Exception {
    Subject subject = new Subject(false, Sets.newHashSet(new KerberosPrincipal(mFooPrincipal)),
        new HashSet<Object>(), new HashSet<Object>());

    // Create login configuration with no Kerberos principal and keytab file.
    LoginModuleConfiguration loginConf = new LoginModuleConfiguration();
    // Kerberos login should fail.
    LoginContext loginContext = new LoginContext("kerberos", subject, null, loginConf);
    mThrown.expect(LoginException.class);
    loginContext.login();
  }
}
