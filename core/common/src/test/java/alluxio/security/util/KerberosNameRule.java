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

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * A rule for the static environment in Kerberos rules during a test suite, including default realm
 * and auth_to_local rules. It sets {@link KerberosName#sDefaultRealm} and
 * {@link KerberosName#sRules} to the specified values during the lifetime of this rule.
 */
public class KerberosNameRule implements TestRule {
  private final String mDefaultRealm;
  private final String mRulesString;

  /**
   * Constructors a new {@link KerberosNameRule}.
   *
   * @param rulesString the auth_to_local rules in string format
   * @param defaultRealm the default realm to set
   */
  public KerberosNameRule(String rulesString, String defaultRealm) {
    mRulesString = rulesString;
    mDefaultRealm = defaultRealm;
  }

  @Override
  public Statement apply(final Statement statement, Description description) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        // TODO(chaomin): create SetAndDestroyKerberosName to simplify the logic here
        KerberosNameTestUtils.setDefaultRealm(mDefaultRealm);
        KerberosNameTestUtils.setRules(mRulesString);
        try {
          statement.evaluate();
        } finally {
          KerberosNameTestUtils.reset();
        }
      }
    };
  }
}
