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

package alluxio.security.authentication;

import javax.annotation.concurrent.ThreadSafe;
import javax.security.sasl.AuthenticationException;

/**
 * An authentication provider implementation that supports Kerberos authentication.
 * The real authentication is conducted via SASL GSSAPI.
 */
@ThreadSafe
public final class KerberosAuthenticationProvider implements AuthenticationProvider  {
  /**
   * Constructs a new Kerberos authentication provider.
   */
  public KerberosAuthenticationProvider() {}

  /**
   * Authenticates using a username and a password. Does nothing, because Kerberose mode does not
   * support username/password login.
   */
  @Override
  public void authenticate(String user, String password) throws AuthenticationException {
    // noop
  }
}

