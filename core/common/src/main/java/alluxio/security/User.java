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

import java.security.Principal;

import javax.annotation.concurrent.ThreadSafe;

/**
 * This class represents a user in Alluxio. It implements {@link java.security.Principal} in the
 * context of Java security frameworks.
 */
@ThreadSafe
public final class User implements Principal {
  private final String mName;

  // ALLUXIO CS REPLACE
  // // TODO(dong): add more attributes and methods for supporting Kerberos
  // ALLUXIO CS WITH
  private final javax.security.auth.Subject mSubject;
  // ALLUXIO CS END

  /**
   * Constructs a new user with a name.
   *
   * @param name the name of the user
   */
  public User(String name) {
    mName = name;
    // ALLUXIO CS ADD
    mSubject = null;
    // ALLUXIO CS END
  }
  // ALLUXIO CS ADD

  /**
   * Constructs a new user with a subject.
   *
   * @param subject the Kerberos subject of the user
   */
  public User(javax.security.auth.Subject subject) {
    mSubject = subject;
    if (subject != null) {
      java.util.Set<javax.security.auth.kerberos.KerberosPrincipal> krb5Principals =
          subject.getPrincipals(javax.security.auth.kerberos.KerberosPrincipal.class);
      if (!krb5Principals.isEmpty()) {
        // TODO(chaomin): for now at most one user is supported in one subject. Consider support
        // multiple Kerberos login users in the future.
        mName = new alluxio.security.util.KerberosName(krb5Principals.iterator().next().toString())
            .getServiceName();
      } else {
        mName = null;
      }
    } else {
      mName = null;
    }
  }
  // ALLUXIO CS END

  @Override
  public String getName() {
    return mName;
  }
  // ALLUXIO CS ADD

  /**
   * @return the subject
   */
  public javax.security.auth.Subject getSubject() {
    return mSubject;
  }
  // ALLUXIO CS END

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof User)) {
      return false;
    }
    User that = (User) o;
    // ALLUXIO CS REPLACE
    // return mName.equals(that.mName);
    // ALLUXIO CS WITH
    return ((mName == that.mName) || ((mName != null) && (mName.equals(that.mName))))
        && ((mSubject == that.mSubject)
            || ((mSubject != null) && (mSubject.equals(that.mSubject))));
    // ALLUXIO CS END
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
