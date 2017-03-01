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
   * Resets the rules and default realm to null.
   */
  public static void resetRulesAndDefaultRealm() {
    synchronized (KerberosName.class) {
      Whitebox.setInternalState(KerberosName.class, "sDefaultRealm", (String) null);
      KerberosName.setRules(null);
    }
  }

  /**
   * Sets the rules and default realm.
   *
   * @param rulesString the rules in string format to set
   * @param defaultRealm the default realm to set
   */
  public static void setRulesAndDefaultRealm(String rulesString, String defaultRealm) {
    synchronized (KerberosName.class) {
      Whitebox.setInternalState(KerberosName.class, "sDefaultRealm", defaultRealm);
      KerberosName.setRules(rulesString);
    }
  }
}
