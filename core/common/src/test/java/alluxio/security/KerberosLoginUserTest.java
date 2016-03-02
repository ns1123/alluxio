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
import alluxio.security.minikdc.MiniKdc;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.File;

/**
 * Unit test for the Kerberos user login.
 */
public final class KerberosLoginUserTest {
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
   * Start the miniKDC.
   */
  @Before
  public void startMiniKdc() throws Exception {
    mWorkDir = mFolder.getRoot();
    mKdc = new MiniKdc(MiniKdc.createConf(), mWorkDir);
    mKdc.start();
  }

  /**
   * Stop the miniKDC.
   */
  @After
  public void stopMiniKdc() {
    if (mKdc != null) {
      mKdc.stop();
    }
  }

  /**
   * Tests the {@link LoginUser} with Kerberos.
   */
  @Test
  public void kerberosLoginUserTest() throws Exception {
    String username = "foo/host";
    String principal = username + "@EXAMPLE.COM";
    File keytab = new File(mWorkDir, "foo.keytab");
    // Create the principal in miniKDC.
    mKdc.createPrincipal(keytab, username);

    Configuration conf = new Configuration();
    conf.set(Constants.SECURITY_AUTHENTICATION_TYPE, "KERBEROS");
    conf.set(Constants.SECURITY_KERBEROS_LOGIN_PRINCIPAL, principal);
    conf.set(Constants.SECURITY_KERBEROS_LOGIN_KEYTAB_FILE, keytab.getPath());

    User loginUser = LoginUser.get(conf);

    Assert.assertNotNull(loginUser);
    Assert.assertEquals(principal, loginUser.getName());
  }
}
