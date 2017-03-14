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

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Unit tests for {@link KerberosName}.
 */
public final class KerberosNameTest {
  /**
   * The exception expected to be thrown.
   */
  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  /**
   * This test verifies {@link KerberosName#KerberosName(String)} parsing with full format of
   * Kerberos principal name.
   */
  @Test
  public void parseFullName() {
    // Add new users into Subject.
    KerberosName name = new KerberosName("foo/localhost@EXAMPLE.COM");
    Assert.assertEquals("foo", name.getServiceName());
    Assert.assertEquals("localhost", name.getHostName());
    Assert.assertEquals("EXAMPLE.COM", name.getRealm());
  }

  /**
   * This test verifies {@link KerberosName#KerberosName(String)} parsing Kerberos principal name
   * without hostname.
   */
  @Test
  public void parseNameWithoutHostname() {
    // Add new users into Subject.
    KerberosName name = new KerberosName("foo@EXAMPLE.COM");
    Assert.assertEquals("foo", name.getServiceName());
    Assert.assertNull(name.getHostName());
    Assert.assertEquals("EXAMPLE.COM", name.getRealm());
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
    KerberosName name = new KerberosName("foo@bar@EXAMPLE.COM");
  }
}
