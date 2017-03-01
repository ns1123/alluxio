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

import org.powermock.reflect.Whitebox;

/**
 * Utility methods for the tests using {@link KerberosName}.
 */
public final class KerberosNameTestUtils {

  private KerberosNameTestUtils() {} // prevent instantiation

  /**
   * Sets the default realm.
   *
   * @param defaultRealm the default realm to set
   */
  public static void setDefaultRealm(String defaultRealm) {
    synchronized (KerberosName.class) {
      Whitebox.setInternalState(KerberosName.class, "sDefaultRealm", defaultRealm);
    }
  }

  /**
   * Sets the auth_to_local rules.
   *
   * @param rulesString the auth_to_local rules in string format
   * @throws Exception if failed to set rules
   */
  public static void setRules(String rulesString) throws Exception {
    synchronized (KerberosName.class) {
      Whitebox.invokeMethod(KerberosName.class, "setRules", rulesString);
    }
  }

  /**
   * Resets the internal state in {@link KerberosName}.
   *
   * @throws Exception if failed to reset the internal state
   */
  public static void reset() throws Exception {
    setDefaultRealm(null);
    setRules(null);
  }
}
