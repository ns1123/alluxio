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

package alluxio.security.authentication;

import javax.annotation.concurrent.ThreadSafe;
// ENTERPRISE ADD
import javax.security.auth.Subject;
// ENTERPRISE END
import javax.security.sasl.AuthenticationException;

/**
 * An authentication provider implementation that allows arbitrary combination of username and
 * password including empty strings.
 */
@ThreadSafe
public final class SimpleAuthenticationProvider implements AuthenticationProvider {

  @Override
  public void authenticate(String user, String password) throws AuthenticationException {
    // no-op authentication
  }

  // ENTERPRISE ADD
  /**
   * Declare authenticate for kerberos, do nothing.
   */
  @Override
  public void authenticate(Subject subject) throws AuthenticationException {}
  // ENTERPRISE END
}
