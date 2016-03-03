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

package alluxio.security.login;

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
   * Starts the miniKDC.
   */
  @Before
  public void before() throws Exception {
    mWorkDir = mFolder.getRoot();
    mKdc = new MiniKdc(MiniKdc.createConf(), mWorkDir);
    mKdc.start();
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
   * Tests the Kerberos {@link LoginModuleConfiguration}.
   */
  @Test
  public void kerberosLoginTest() throws Exception {
    String username = "foo/host";
    String principal = username + "@EXAMPLE.COM";
    File keytab = new File(mWorkDir, "foo.keytab");
    // Create the principal in miniKDC, and generate the keytab file for it.
    mKdc.createPrincipal(keytab, username);

    Subject subject = new Subject(false, Sets.newHashSet(new KerberosPrincipal(principal)),
        new HashSet<Object>(), new HashSet<Object>());
    // Create Kerberos login configuration with principal and keytab file.
    LoginModuleConfiguration loginConf = new LoginModuleConfiguration(
        principal, keytab.getPath());

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
   * Tests the Kerberos Login with invalid principal and keytab combinations.
   */
  @Test
  public void kerberosLoginWithInvalidConfTest() throws Exception {
    String fooUsername = "foo/host";
    String fooPrincipal = fooUsername + "@EXAMPLE.COM";
    File fooKeytab = new File(mWorkDir, "foo.keytab");
    // Create the principal in miniKDC, and generate the keytab file for it.
    mKdc.createPrincipal(fooKeytab, fooUsername);

    String barUsername = "bar/host";
    String barPrincipal = barUsername + "@EXAMPLE.COM";
    File barKeytab = new File(mWorkDir, "bar.keytab");
    // Create the principal in miniKDC, and generate the keytab file for it.
    mKdc.createPrincipal(barKeytab, barUsername);

    // Case 0: Create Kerberos login configuration with foo principal and bar keytab file.
    Subject subject = new Subject(false, Sets.newHashSet(new KerberosPrincipal(fooPrincipal)),
        new HashSet<Object>(), new HashSet<Object>());
    LoginModuleConfiguration loginConf = new LoginModuleConfiguration(
        fooPrincipal, barKeytab.getPath());
    // Kerberos login should fail.
    LoginContext loginContext = new LoginContext("kerberos", subject, null, loginConf);
    mThrown.expect(LoginException.class);
    loginContext.login();

    // Case 1: Create Kerberos login configuration with bar principal and foo keytab file.
    subject = new Subject(false, Sets.newHashSet(new KerberosPrincipal(barPrincipal)),
        new HashSet<Object>(), new HashSet<Object>());
    loginConf = new LoginModuleConfiguration(barPrincipal, fooKeytab.getPath());
    // Kerberos login should fail.
    loginContext = new LoginContext("kerberos", subject, null, loginConf);
    mThrown.expect(LoginException.class);
    loginContext.login();

    // Case 2: Create Kerberos login configuration with bar principal and invalid keytab file.
    subject = new Subject(false, Sets.newHashSet(new KerberosPrincipal(barPrincipal)),
        new HashSet<Object>(), new HashSet<Object>());
    loginConf = new LoginModuleConfiguration(barPrincipal, barKeytab.getPath() + ".invalid");
    // Kerberos login should fail.
    loginContext = new LoginContext("kerberos", subject, null, loginConf);
    mThrown.expect(LoginException.class);
    loginContext.login();

    // Case 3: Create Kerberos login configuration with non-existing principal.
    String nonexistPrincipal = "nonexist/host@EXAMPLE.COM";
    subject = new Subject(false, Sets.newHashSet(new KerberosPrincipal(nonexistPrincipal)),
        new HashSet<Object>(), new HashSet<Object>());
    loginConf = new LoginModuleConfiguration(nonexistPrincipal, fooKeytab.getPath());
    // Kerberos login should fail.
    loginContext = new LoginContext("kerberos", subject, null, loginConf);
    mThrown.expect(LoginException.class);
    loginContext.login();
  }

  /**
   * Tests the Kerberos Login with missing principal and keytab.
   */
  @Test
  public void kerberosLoginWithMissingConfTest() throws Exception {
    String principal = "foo/host@EXAMPLE.COM";
    Subject subject = new Subject(false, Sets.newHashSet(new KerberosPrincipal(principal)),
        new HashSet<Object>(), new HashSet<Object>());

    // Create login configuration with no Kerberos principal and keytab file.
    LoginModuleConfiguration loginConf = new LoginModuleConfiguration();
    // Kerberos login should fail.
    LoginContext loginContext = new LoginContext("kerberos", subject, null, loginConf);
    mThrown.expect(LoginException.class);
    loginContext.login();
  }
}
