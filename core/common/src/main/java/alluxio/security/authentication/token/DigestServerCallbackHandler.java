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

package alluxio.security.authentication.token;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.RealmCallback;
import java.io.IOException;

/**
 * Abstract server callback handler for DIGEST-MD5 based authentication.
 */
public abstract class DigestServerCallbackHandler implements CallbackHandler {
  private static final Logger LOG = LoggerFactory.getLogger(DigestServerCallbackHandler.class);

  protected abstract void authorize(AuthorizeCallback ac) throws IOException;

  protected abstract char[] getPassword(String name);

  @Override
  public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
    AuthorizeCallback ac = null;
    NameCallback nc = null;
    PasswordCallback pc = null;
    // Iterate over given callbacks for callback activation.
    // We need to do an initial pass since callbacks may depend on each other.
    for (Callback callback : callbacks) {
      if (callback instanceof AuthorizeCallback) {
        ac = (AuthorizeCallback) callback;
      } else if (callback instanceof NameCallback) {
        nc = (NameCallback) callback;
      } else if (callback instanceof PasswordCallback) {
        pc = (PasswordCallback) callback;
      } else if (callback instanceof RealmCallback) {
        // ignore realms
      } else {
        throw new UnsupportedCallbackException(callback,
            String.format("Unrecognized SASL DIGEST Callback: %s", callback.getClass()));
      }
    }

    if (pc != null) {
      Preconditions.checkArgument(nc != null, "NameCallback missing for AuthorizeCallback.");
      LOG.debug("DigestServerCallbackHandler: Retrieving password for {}", nc.getDefaultName());
      char[] password = getPassword(nc.getDefaultName());
      pc.setPassword(password);
    }

    if (ac != null) {
      authorize(ac);
      if (ac.isAuthorized()) {
        LOG.debug("DigestServerCallbackHandler: user {} successfully authorized",
            ac.getAuthorizedID());
      }
    }
  }
}
