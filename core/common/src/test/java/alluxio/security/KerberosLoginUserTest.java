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
   * Tests the {@link LoginUser} with valid Kerberos principal and keytab.
   */
  @Test
  public void kerberosLoginUserTest() throws Exception {
    String username = "foo/host";
    String principal = username + "@EXAMPLE.COM";
    File keytab = new File(mWorkDir, "foo.keytab");
    // Create the principal in miniKDC, and generate the keytab file for it.
    mKdc.createPrincipal(keytab, username);

    Configuration conf = new Configuration();
    conf.set(Constants.SECURITY_AUTHENTICATION_TYPE, AuthType.KERBEROS.getAuthName());
    conf.set(Constants.SECURITY_KERBEROS_LOGIN_PRINCIPAL, principal);
    conf.set(Constants.SECURITY_KERBEROS_LOGIN_KEYTAB_FILE, keytab.getPath());

    User loginUser = LoginUser.get(conf);

    Assert.assertNotNull(loginUser);
    Assert.assertEquals(principal, loginUser.getName());
  }

  /**
   * Tests the {@link LoginUser} with invalid principal and keytab file combination.
   */
  @Test
  public void kerberosLoginUserWithInvalidFieldsTest() throws Exception {
    String fooUsername = "foo/host";
    String fooPrincipal = fooUsername + "@EXAMPLE.COM";
    File fooKeytab = new File(mWorkDir, "foo.keytab");
    // Create foo principal in miniKDC, and generate the keytab file for it.
    mKdc.createPrincipal(fooKeytab, fooUsername);

    String barUsername = "bar/host";
    String barPrincipal = barUsername + "@EXAMPLE.COM";
    File barKeytab = new File(mWorkDir, "bar.keytab");
    // Create bar principal in miniKDC, and generate the keytab file for it.
    mKdc.createPrincipal(barKeytab, barUsername);

    // Case 0: Create Kerberos login configuration with foo principal and bar keytab file.
    Configuration conf = new Configuration();
    conf.set(Constants.SECURITY_AUTHENTICATION_TYPE, AuthType.KERBEROS.getAuthName());
    conf.set(Constants.SECURITY_KERBEROS_LOGIN_PRINCIPAL, fooPrincipal);
    conf.set(Constants.SECURITY_KERBEROS_LOGIN_KEYTAB_FILE, barKeytab.getPath());
    mThrown.expect(IOException.class);
    LoginUser.get(conf);

    // Case 1: Create Kerberos login configuration with bar principal and foo keytab file.
    conf = new Configuration();
    conf.set(Constants.SECURITY_AUTHENTICATION_TYPE, AuthType.KERBEROS.getAuthName());
    conf.set(Constants.SECURITY_KERBEROS_LOGIN_PRINCIPAL, barPrincipal);
    conf.set(Constants.SECURITY_KERBEROS_LOGIN_KEYTAB_FILE, fooKeytab.getPath());
    mThrown.expect(IOException.class);
    LoginUser.get(conf);

    // Case 2: Create Kerberos login configuration with bar principal and invalid keytab file.
    conf = new Configuration();
    conf.set(Constants.SECURITY_AUTHENTICATION_TYPE, AuthType.KERBEROS.getAuthName());
    conf.set(Constants.SECURITY_KERBEROS_LOGIN_PRINCIPAL, barPrincipal);
    conf.set(Constants.SECURITY_KERBEROS_LOGIN_KEYTAB_FILE, fooKeytab.getPath() + ".invalid");
    mThrown.expect(IOException.class);
    LoginUser.get(conf);

    // Case 3: Create Kerberos login configuration with non-existing princial.
    String nonexistPrincipal = "nonexist/host@EXAMPLE.COM";
    conf = new Configuration();
    conf.set(Constants.SECURITY_AUTHENTICATION_TYPE, AuthType.KERBEROS.getAuthName());
    conf.set(Constants.SECURITY_KERBEROS_LOGIN_PRINCIPAL, nonexistPrincipal);
    conf.set(Constants.SECURITY_KERBEROS_LOGIN_KEYTAB_FILE, fooKeytab.getPath());
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
}
