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

package alluxio.security.util;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Unit tests for {@link KerberosName}.
 */
public final class KerberosNameTest {
  private static final String TEST_REALM = "EXAMPLE.COM";
  /**
   * The exception expected to be thrown.
   */
  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @Before
  public void before() throws Exception {
    System.setProperty("java.security.krb5.realm", TEST_REALM);
    System.setProperty("java.security.krb5.kdc", "localhost:88");

    String rules = "RULE:[1:$1@$0](.*@GOOGLE\\.COM)s/@.*//\n"
        + "RULE:[2:$1](alice)s/^.*$/guest/\n"
        + "RULE:[2:$1;$2](^.*;admin$)s/;admin$//\n"
        + "RULE:[2:$2](root)\n"
        + "DEFAULT";
    KerberosName.setRules(rules);
  }

  @After
  public void after() {
    KerberosName.setRules("DEFAULT");
    System.clearProperty("java.security.krb5.realm");
    System.clearProperty("java.security.krb5.kdc");
  }

  /**
   * This test verifies {@link KerberosName#KerberosName(String)} parsing with full format of
   * Kerberos principal name.
   */
  @Test
  public void parseFullName() {
    // Add new users into Subject.
    KerberosName name = new KerberosName("foo/localhost@" + TEST_REALM);
    Assert.assertEquals("foo", name.getServiceName());
    Assert.assertEquals("localhost", name.getHostName());
    Assert.assertEquals(TEST_REALM, name.getRealm());
  }

  /**
   * This test verifies {@link KerberosName#KerberosName(String)} parsing Kerberos principal name
   * without hostname.
   */
  @Test
  public void parseNameWithoutHostname() {
    // Add new users into Subject.
    KerberosName name = new KerberosName("foo@" + TEST_REALM);
    Assert.assertEquals("foo", name.getServiceName());
    Assert.assertNull(name.getHostName());
    Assert.assertEquals(TEST_REALM, name.getRealm());
  }

  /**
   * This test verifies {@link KerberosName#KerberosName(String)} parsing Kerberos principal name
   * with no hostname and no realm.
   */
  @Test
  public void parseNameWithoutHostnameAndRealm() {
    // Add new users into Subject.
    KerberosName name = new KerberosName("foo");
    Assert.assertEquals("foo", name.getServiceName());
    Assert.assertNull(name.getHostName());
    Assert.assertNull(name.getRealm());
  }

  /**
   * This test verifies {@link KerberosName#KerberosName(String)} parsing invalid Kerberos
   * principal name multiple '@'.
   */
  @Test
  public void parseInvalidNameWithMultipleAt() {
    // Add new users into Subject.
    mThrown.expect(IllegalArgumentException.class);
    KerberosName name = new KerberosName("foo@bar@" + TEST_REALM);
  }

  @Test
  public void testRulesWithValidInput() throws Exception {
    testTranslation("bob@" + TEST_REALM, "bob");
    testTranslation("alluxio/127.0.0.1@" + TEST_REALM, "alluxio");
    testTranslation("larry@GOOGLE.COM", "larry");
    testTranslation("alice/random@FOO.COM", "guest");
    testTranslation("jack/admin@FOO.COM", "jack");
    testTranslation("jack/root@FOO.COM", "root");
  }

  @Test
  public void testInvalidKerberosNames() throws Exception {
    failTranslation("charlie@NONDEFAULTREALM.COM");
    failTranslation("root/hostname@NONEXISTINGREALM.COM");
  }

  private void testTranslation(String from, String to) throws Exception {
    KerberosName kerberosName = new KerberosName(from);
    String shortName = kerberosName.getShortName();
    Assert.assertEquals("Translated to wrong short name", to, shortName);
  }

  private void failTranslation(String from) {
    KerberosName kerberosName = new KerberosName(from);
    try {
      kerberosName.getShortName();
      Assert.fail("Get short name for " + from + " should fail.");
    } catch (Exception e) {
      // expected
    }
  }
}
