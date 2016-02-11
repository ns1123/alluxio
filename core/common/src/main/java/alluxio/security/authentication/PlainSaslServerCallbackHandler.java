/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package alluxio.security.authentication;

import java.io.IOException;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;

/**
 * A callback that is used by the SASL mechanisms to get further information to
 * complete the authentication. For example, a SASL mechanism might use this callback handler to
 * do verification operation.
 */
public final class PlainSaslServerCallbackHandler implements CallbackHandler {
  private final AuthenticationProvider mAuthenticationProvider;

  /**
   * Constructs a new callback handler.
   *
   * @param authenticationProvider the authentication provider used
   */
  public PlainSaslServerCallbackHandler(AuthenticationProvider authenticationProvider) {
    mAuthenticationProvider = authenticationProvider;
  }

  @Override
  public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
    String username = null;
    String password = null;
    AuthorizeCallback ac = null;

    for (Callback callback : callbacks) {
      if (callback instanceof NameCallback) {
        NameCallback nc = (NameCallback) callback;
        username = nc.getName();
      } else if (callback instanceof PasswordCallback) {
        PasswordCallback pc = (PasswordCallback) callback;
        password = new String(pc.getPassword());
      } else if (callback instanceof AuthorizeCallback) {
        ac = (AuthorizeCallback) callback;
      } else {
        throw new UnsupportedCallbackException(callback, "Unsupport callback");
      }
    }

    mAuthenticationProvider.authenticate(username, password);

    if (ac != null) {
      ac.setAuthorized(true);

      // After verification succeeds, a user with this authz id will be set to a Threadlocal.
      AuthenticationUtils.AuthorizedClientUser.set(ac.getAuthorizedID());
    }
  }
}
