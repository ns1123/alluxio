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

/**
 * Unit test for the kerberos login module used in {@link LoginModuleConfiguration}.
 */
public class KerberosLoginModuleTest {
  private MiniKdc mKdc;
  private File mWorkDir;

  /**
   * The exception expected to be thrown.
   */
  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  /**
   * Temporary folder for minikdc keytab files.
   */
  @Rule
  public final TemporaryFolder mFolder = new TemporaryFolder();

  /**
   * Start the minikdc.
   * @throws Exception
   */
  @Before
  public void startMiniKdc() throws Exception {
    mWorkDir = mFolder.getRoot();
    mKdc = new MiniKdc(MiniKdc.createConf(), mWorkDir);
    mKdc.start();
  }

  /**
   * Stop the minikdc.
   * @throws Exception
   */
  @After
  public void stopMiniKdc() {
    if (mKdc != null) {
      mKdc.stop();
    }
  }

  /**
   * Tests the Kerberos LoginModuleConfiguration.
   * @throws Exception
   */
  @Test
  public void kerberosLoginTest() throws Exception {
    String principal = "foo/host";
    File keytab = new File(mWorkDir, "foo.keytab");
    // Create the principal in minikdc.
    mKdc.createPrincipal(keytab, principal);

    Subject subject = new Subject(false, Sets.newHashSet(new KerberosPrincipal(principal)),
        new HashSet<Object>(), new HashSet<Object>());
    // Create kerberos login configuration with principal and keytab file.
    LoginModuleConfiguration loginConf = new LoginModuleConfiguration(
        principal, keytab.getPath());

    // kerberos login.
    LoginContext loginContext = new LoginContext("kerberos", subject, null, loginConf);
    loginContext.login();

    // Verify the login result.
    Assert.assertFalse(subject.getPrincipals(KerberosPrincipal.class).isEmpty());
    Assert.assertEquals("[foo/host@EXAMPLE.COM]",
        subject.getPrincipals(KerberosPrincipal.class).toString());

    // logout and verify the user is removed
    loginContext.logout();
    Assert.assertTrue(subject.getPrincipals(KerberosPrincipal.class).isEmpty());

    // logout twice should be no-op.
    loginContext.logout();
    Assert.assertTrue(subject.getPrincipals(KerberosPrincipal.class).isEmpty());
  }
}
