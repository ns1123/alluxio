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

import org.junit.Assert;
import org.junit.Test;

import java.util.Set;

import javax.security.auth.Subject;

/**
 * Unit test for {@link User}.
 */
public final class UserTest {

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
    Assert.assertEquals(2, users.size());

    // Test equals.
    Assert.assertTrue(users.contains(new User("realUser")));
    Assert.assertFalse(users.contains(new User("noExistingUser")));
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
    Assert.assertEquals(3, users.size());

    // Add similar user name without domain name.
    subject.getPrincipals().add(new User("admin"));
    subject.getPrincipals().add(new User("imap"));

    users = subject.getPrincipals(User.class);
    Assert.assertEquals(5, users.size());
  }
  // ALLUXIO CS ADD

  @Test
  public void emptySubjectTest() {
    try {
      Subject subject = new Subject();
      User user = new User(subject);
      Assert.fail("creating User from an empty subject should fail");
    } catch (Exception e) {
      // Expected
    }
  }

  /**
   * Tests for {@link User#User(Subject)}, {@link User#getSubject()} and
   * {@link User#equals(Object)} in KERBEROS mode.
   */
  @Test
  public void kerberosSubjectTest() {
    // No principal in subject.
    Subject subject = new Subject();

    // One principal in subject.
    subject.getPrincipals().add(
        new javax.security.auth.kerberos.KerberosPrincipal("foo/admin@EXAMPLE.COM"));
    User user = new User(subject);
    Assert.assertNotNull(user.getSubject());
    Assert.assertEquals("[foo/admin@EXAMPLE.COM]",
        user.getSubject().getPrincipals(
            javax.security.auth.kerberos.KerberosPrincipal.class).toString());
    Assert.assertEquals("foo", user.getName());

    // Test equals.
    Assert.assertTrue(user.equals(user));
    Assert.assertFalse(user.equals(null));

    // Two principal in subject, for now User only takes the first principal as the user name.
    subject.getPrincipals().add(
        new javax.security.auth.kerberos.KerberosPrincipal("bar/admin@EXAMPLE.COM"));
    user = new User(subject);
    Assert.assertNotNull(user.getSubject());
    Assert.assertEquals("[foo/admin@EXAMPLE.COM, bar/admin@EXAMPLE.COM]",
        user.getSubject().getPrincipals(
            javax.security.auth.kerberos.KerberosPrincipal.class).toString());
    Assert.assertEquals("foo", user.getName());

    // Test Equals.
    Assert.assertTrue(user.equals(user));
    Assert.assertFalse(user.equals(null));
  }
  // ALLUXIO CS END
}
