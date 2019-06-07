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

import alluxio.security.authentication.token.TokenUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import java.io.IOException;
import java.util.HashMap;

/**
 * {@link SaslServerHandler} implementation for DelegationToken scheme.
 */
public class SaslServerHandlerDelegationToken implements SaslServerHandler {
  private static final Logger LOG = LoggerFactory.getLogger(SaslServerHandlerDelegationToken.class);

  /** Underlying {@code SaslServer}. */
  private final SaslServer mSaslServer;

  /** Authenticated user info. */
  private AuthenticatedUserInfo mUserInfo;

  /**
   * Creates {@link SaslServerHandler} for Plain/Custom.
   *
   * @param serverName server name
   * @param tokenManager delegation token manager
   * @throws SaslException
   */
  public SaslServerHandlerDelegationToken(String serverName, DelegationTokenManager tokenManager)
      throws SaslException {
    mSaslServer = Sasl.createSaslServer(TokenUtils.DIGEST_MECHANISM_NAME,
        TokenUtils.TOKEN_PROTOCOL_NAME, serverName, new HashMap<String, String>(),
        new DigestServerCallbackHandlerDelegation(this, tokenManager));
  }

  @Override
  public AuthenticatedUserInfo getAuthenticatedUserInfo() {
    return mUserInfo;
  }

  @Override
  public void close() throws IOException {
    if (mSaslServer != null) {
      try {
        mSaslServer.dispose();
      } catch (SaslException exc) {
        LOG.debug("Failed to close SaslClient.", exc);
      }
    }
  }

  @Override
  public SaslServer getSaslServer() {
    return mSaslServer;
  }

  @Override
  public void setAuthenticatedUserInfo(AuthenticatedUserInfo userinfo) {
    mUserInfo = userinfo;
  }
}
