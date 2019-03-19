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

import alluxio.security.authentication.Token;

import com.google.common.base.Charsets;
import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.RealmCallback;
import javax.security.sasl.RealmChoiceCallback;

/**
 * Client callback handler for DIGEST-MD5 based authentication.
 */
class DigestClientCallbackHandler implements CallbackHandler {
  private static final Logger LOG = LoggerFactory.getLogger(DigestClientCallbackHandler.class);

  private final String mUserName;
  private final char[] mUserPassword;

  /**
   * @param token token used for authentication
   */
  public DigestClientCallbackHandler(Token<?> token) {
    mUserName = buildUserName(token);
    mUserPassword = buildPassword(token);
  }

  private static String buildUserName(Token<?> token) {
    byte[] proto = token.getId().getBytes();
    return new String(Base64.encodeBase64(proto, false), Charsets.UTF_8);
  }

  private char[] buildPassword(Token<?> token) {
    return new String(Base64.encodeBase64(token.getPassword(), false), Charsets.UTF_8)
        .toCharArray();
  }

  @Override
  public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
    NameCallback nc = null;
    PasswordCallback pc = null;
    RealmCallback rc = null;
    for (Callback callback : callbacks) {
      if (callback instanceof RealmChoiceCallback) {
        continue;
      } else if (callback instanceof NameCallback) {
        nc = (NameCallback) callback;
      } else if (callback instanceof PasswordCallback) {
        pc = (PasswordCallback) callback;
      } else if (callback instanceof RealmCallback) {
        rc = (RealmCallback) callback;
      } else {
        throw new UnsupportedCallbackException(callback,
            String.format("Unrecognized SASL DIGEST callback: %s", callback.getClass()));
      }
    }
    if (nc != null) {
      LOG.debug("DigestClientCallbackHandler: setting username to {}", mUserName);
      nc.setName(mUserName);
    }
    if (pc != null) {
      LOG.debug("DigestClientCallbackHandler: setting password");
      pc.setPassword(mUserPassword);
    }
    if (rc != null) {
      LOG.debug("DigestClientCallbackHandler: setting realm to {}", rc.getDefaultText());
      rc.setText(rc.getDefaultText());
    }
  }
}
