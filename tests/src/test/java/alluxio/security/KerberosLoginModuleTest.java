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

import alluxio.BaseIntegrationTest;
import alluxio.ConfigurationTestUtils;
import alluxio.security.login.LoginModuleConfiguration;
import alluxio.security.minikdc.MiniKdc;

import com.google.common.collect.Sets;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.HashSet;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

/**
 * Unit test for the Kerberos login module.
 */
public final class KerberosLoginModuleTest extends BaseIntegrationTest {
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
  }

  @AfterClass
  public static void afterClass() throws Exception {
    if (sKdc != null) {
      sKdc.stop();
    }
  }

  public void after() throws Exception {
    ConfigurationTestUtils.resetConfiguration();
    LoginUserTestUtils.resetLoginUser();
  }

  /**
   * Tests the Kerberos {@link LoginModuleConfiguration}.
   */
  @Test
  public void kerberosLoginTest() throws Exception {
    Subject subject = new Subject(false, Sets.newHashSet(new KerberosPrincipal(sFooPrincipal)),
        new HashSet<>(), new HashSet<>());
    // Create Kerberos login configuration with principal and keytab file.
    LoginModuleConfiguration loginConf = new LoginModuleConfiguration(
        sFooPrincipal, sFooKeytab.getPath());

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
    Subject subject = new Subject(false, Sets.newHashSet(new KerberosPrincipal(sFooPrincipal)),
        new HashSet<>(), new HashSet<>());
    LoginModuleConfiguration loginConf = new LoginModuleConfiguration(
        sFooPrincipal, sBarKeytab.getPath());
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
    Subject subject = new Subject(false, Sets.newHashSet(new KerberosPrincipal(sFooPrincipal)),
        new HashSet<>(), new HashSet<>());
    LoginModuleConfiguration loginConf = new LoginModuleConfiguration(
        sFooPrincipal, sFooKeytab.getPath() + ".invalid");
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
        new HashSet<>(), new HashSet<>());
    LoginModuleConfiguration loginConf = new LoginModuleConfiguration(
        nonexistPrincipal, sFooKeytab.getPath());
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
    Subject subject = new Subject(false, Sets.newHashSet(new KerberosPrincipal(sFooPrincipal)),
        new HashSet<>(), new HashSet<>());

    // Create login configuration with no Kerberos principal and keytab file.
    LoginModuleConfiguration loginConf = new LoginModuleConfiguration();
    // Kerberos login should fail.
    LoginContext loginContext = new LoginContext("kerberos", subject, null, loginConf);
    mThrown.expect(LoginException.class);
    loginContext.login();
  }
}
