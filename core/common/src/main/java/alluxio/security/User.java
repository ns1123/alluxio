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

import java.security.Principal;
// ENTERPRISE ADD
import java.util.Set;
// ENTERPRISE END

import javax.annotation.concurrent.ThreadSafe;
// ENTERPRISE ADD
import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
// ENTERPRISE END

/**
 * This class represents a user in Alluxio. It implements {@link java.security.Principal} in the
 * context of Java security frameworks.
 */
@ThreadSafe
public final class User implements Principal {
  private final String mName;

  // ENTERPRISE EDIT
  private final Subject mSubject;
  // ENTERPRISE REPLACES
  // // TODO(dong): add more attributes and methods for supporting Kerberos
  // ENTERPRISE END

  /**
   * Constructs a new user with a name.
   *
   * @param name the name of the user
   */
  public User(String name) {
    mName = name;
    // ENTERPRISE ADD
    mSubject = null;
    // ENTERPRISE END
  }
  // ENTERPRISE ADD

  /**
   * Constructs a new user with a subject.
   *
   * @param subject the Kerberos subject of the user
   */
  public User(Subject subject) {
    mSubject = subject;
    if (subject != null) {
      Set<KerberosPrincipal> krb5Principals = subject.getPrincipals(KerberosPrincipal.class);
      if (!krb5Principals.isEmpty()) {
        // TODO(chaomin): for now at most one user is supported in one subject. Consider support
        // multiple Kerberos login users in the future.
        mName = krb5Principals.iterator().next().toString();
      } else {
        mName = null;
      }
    } else {
      mName = null;
    }
  }
  // ENTERPRISE END

  @Override
  public String getName() {
    return mName;
  }
  // ENTERPRISE ADD

  /**
   * @return the subject
   */
  public Subject getSubject() {
    return mSubject;
  }
  // ENTERPRISE END

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof User)) {
      return false;
    }
    User that = (User) o;
    // ENTERPRISE EDIT
    return ((mName == that.mName) || ((mName != null) && (mName.equals(that.mName))))
        && ((mSubject == that.mSubject)
            || ((mSubject != null) && (mSubject.equals(that.mSubject))));
    // ENTERPRISE REPLACES
    // return mName.equals(that.mName);
    // ENTERPRISE END
  }

  @Override
  public int hashCode() {
    return mName.hashCode();
  }

  @Override
  public String toString() {
    return mName;
  }
}
