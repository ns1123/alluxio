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

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;

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
   * @param conf Alluxio configuration
   * @throws java.io.IOException if failed to parse Kerberos name to short name
   * @throws javax.security.auth.login.LoginException if the login failed
   */
  public User(javax.security.auth.Subject subject, AlluxioConfiguration conf)
      throws java.io.IOException,
      javax.security.auth.login.LoginException {
    mSubject = subject;
    if (subject != null) {
      if (Boolean.getBoolean("sun.security.jgss.native")
          && alluxio.util.CommonUtils.isAlluxioServer()) {
        mName = alluxio.security.util.KerberosUtils
            .getKerberosServiceName(conf);
        return;
      }
      // Obtains name from Kerberos principal in the subject if available.
      if (!subject.getPrincipals(javax.security.auth.kerberos.KerberosPrincipal.class).isEmpty()
          || Boolean.getBoolean("sun.security.jgss.native")) {
        alluxio.security.util.KerberosName kerberosName =
            alluxio.security.util.KerberosUtils.extractKerberosNameFromSubject(subject);
        com.google.common.base.Preconditions.checkNotNull(kerberosName);
        mName = kerberosName.getShortName(conf.get(PropertyKey.SECURITY_KERBEROS_AUTH_TO_LOCAL));
        return;
      }
      // Obtains name from Alluxio user in the subject if available.
      // This is available when delegation token is used.
      mName = subject.getPrincipals(User.class).stream().findFirst().map(User::getName).orElse(null);
    } else {
      mName = null;
    }
    if (mName == null) {
      throw new javax.security.auth.login.LoginException(
          String.format("Unable to retrieve user name from subject: %s.", subject));
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
  @javax.annotation.Nullable
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
