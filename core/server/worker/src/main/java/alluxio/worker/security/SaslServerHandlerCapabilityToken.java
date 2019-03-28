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

package alluxio.worker.security;

import alluxio.security.authentication.AuthenticatedUserInfo;
import alluxio.security.authentication.AuthenticationServer;
import alluxio.security.authentication.SaslServerHandler;
import alluxio.security.authentication.token.TokenUtils;

import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import java.util.HashMap;
import java.util.UUID;

/**
 * {@link SaslServerHandler} implementation for Capability scheme.
 */
public class SaslServerHandlerCapabilityToken implements SaslServerHandler {
  /** Underlying {@code SaslServer}. */
  private final SaslServer mSaslServer;
  /** Underlying capability cache. */
  private final CapabilityCache mCapabilityCache;
  /** Authenticated user info. */
  private AuthenticatedUserInfo mUserInfo;

  /**
   * Creates {@link SaslServerHandler} for Plain/Custom.
   *
   * @param serverName server name
   * @param capabilityCache capability cache
   * @throws SaslException
   */
  public SaslServerHandlerCapabilityToken(String serverName, CapabilityCache capabilityCache)
      throws SaslException {
    mCapabilityCache = capabilityCache;
    mSaslServer =
        Sasl.createSaslServer(TokenUtils.DIGEST_MECHANISM_NAME, TokenUtils.TOKEN_PROTOCOL_NAME,
            serverName, new HashMap<String, String>(), new DigestServerCallbackHandlerCapability(
                this, mCapabilityCache.getActiveCapabilityKeys()));
  }

  @Override
  public void authenticationCompleted(UUID channelId, AuthenticationServer authenticationServer) {
    authenticationServer.registerChannel(channelId, mUserInfo, mSaslServer);
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