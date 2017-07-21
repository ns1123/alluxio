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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import java.util.Set;

import javax.security.auth.Subject;

/**
 * Unit test for {@link User}.
 */
public final class UserTest {
  // ALLUXIO CS ADD
  private static final String TEST_REALM = "EXAMPLE.COM";

  @org.junit.Rule
  public alluxio.security.util.KerberosNameRule mKerberosNameRule =
      new alluxio.security.util.KerberosNameRule(alluxio.Constants.KERBEROS_DEFAULT_AUTH_TO_LOCAL,
          TEST_REALM);
  // ALLUXIO CS END

  /**
   * This test verifies whether the {@link User} could be used in Java security
   * framework.
   */
  @Test
  public void usedInSecurityContext() {
    // Add new users into Subject.
    Subject subject = new Subject();
    subject.getPrincipals().add(new User("realUser"));
    subject.getPrincipals().add(new User("proxyUser"));

    // Fetch added users.
    Set<User> users = subject.getPrincipals(User.class);

    // Verification.
    assertEquals(2, users.size());

    // Test equals.
    assertTrue(users.contains(new User("realUser")));
    assertFalse(users.contains(new User("noExistingUser")));
  }

  /**
   * This test verifies that full realm format is valid as {@link User} name.
   */
  @Test
  public void realmAsUserName() {
    // Add new users into Subject.
    Subject subject = new Subject();
    subject.getPrincipals().add(new User("admin/admin@EXAMPLE.com"));
    subject.getPrincipals().add(new User("admin/mbox.example.com@EXAMPLE.com"));
    subject.getPrincipals().add(new User("imap/mbox.example.com@EXAMPLE.COM"));

    // Fetch added users.
    Set<User> users = subject.getPrincipals(User.class);
    assertEquals(3, users.size());

    // Add similar user name without domain name.
    subject.getPrincipals().add(new User("admin"));
    subject.getPrincipals().add(new User("imap"));

    users = subject.getPrincipals(User.class);
    assertEquals(5, users.size());
  }
  // ALLUXIO CS ADD

  @Test
  public void emptySubjectTest() {
    try {
      Subject subject = new Subject();
      User user = new User(subject);
      org.junit.Assert.fail("creating User from an empty subject should fail");
    } catch (Exception e) {
      // Expected
    }
  }

  /**
   * Tests for {@link User#User(Subject)}, {@link User#getSubject()} and
   * {@link User#equals(Object)} in KERBEROS mode.
   */
  @Test
  public void kerberosSubjectTest() throws Exception {
    // No principal in subject.
    Subject subject = new Subject();

    // One principal in subject.
    subject.getPrincipals().add(
        new javax.security.auth.kerberos.KerberosPrincipal("foo/admin@" + TEST_REALM));
    User user = new User(subject);
    org.junit.Assert.assertNotNull(user.getSubject());
    assertEquals("[foo/admin@EXAMPLE.COM]",
        user.getSubject().getPrincipals(
            javax.security.auth.kerberos.KerberosPrincipal.class).toString());
    assertEquals("foo", user.getName());

    // Test equals.
    assertTrue(user.equals(user));
    assertFalse(user.equals(null));

    // Two principal in subject, for now User only takes the first principal as the user name.
    subject.getPrincipals().add(
        new javax.security.auth.kerberos.KerberosPrincipal("bar/admin@" + TEST_REALM));
    user = new User(subject);
    org.junit.Assert.assertNotNull(user.getSubject());
    assertEquals(String.format("[foo/admin@%s, bar/admin@%s]", TEST_REALM, TEST_REALM),
        user.getSubject().getPrincipals(
            javax.security.auth.kerberos.KerberosPrincipal.class).toString());
    assertEquals("foo", user.getName());

    // Test Equals.
    assertTrue(user.equals(user));
    assertFalse(user.equals(null));
  }
  // ALLUXIO CS END
}
