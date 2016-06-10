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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Util class to parse and manipulate Kerberos principal name.
 *
 * A typical Kerberos principal name has the following format:
 *   [serviceName]/[hostname]@[realm]
 * If the [hostname] is empty, it will be [serviceName]@[realm].
 */
@ThreadSafe
public final class KerberosName {
  private static final Logger LOG = LoggerFactory.getLogger(KerberosName.class);
  private final String mServiceName;
  private final String mHostName;
  private final String mRealm;
  private final Pattern mNamePattern = Pattern.compile("([^/@]*)(/([^/@]*))?@([^/@]*)");

  /**
   * Constructs a KerberosName with a principal name string.
   *
   * @param name Kerberos principal name
   */
  public KerberosName(String name) {
    Matcher match = mNamePattern.matcher(name);
    if (!match.matches()) {
      if (name.contains("@")) {
        throw new IllegalArgumentException("Malformed Kerberos name: " + name);
      }
      mServiceName = name;
      mHostName = null;
      mRealm = null;
    } else {
      mServiceName = match.group(1);
      mHostName = match.group(3);
      mRealm = match.group(4);
    }

  }

  /**
   * @return the string format of the Kerberos name
   */
  public String toString() {
    StringBuilder result = new StringBuilder();
    result.append(mServiceName);
    if (mHostName != null) {
      result.append('/');
      result.append(mHostName);
    }

    if (mRealm != null) {
      result.append('@');
      result.append(mRealm);
    }

    return result.toString();
  }

  /**
   * @return the service name
   */
  public String getServiceName() {
    return mServiceName;
  }

  /**
   * @return the hostname
   */
  public String getHostName() {
    return mHostName;
  }

  /**
   * @return the realm
   */
  public String getRealm() {
    return mRealm;
  }
}
