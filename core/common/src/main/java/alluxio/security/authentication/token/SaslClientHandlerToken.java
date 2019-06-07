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

import alluxio.exception.status.UnauthenticatedException;
import alluxio.grpc.ChannelAuthenticationScheme;
import alluxio.security.authentication.DelegationTokenIdentifier;
import alluxio.security.authentication.SaslClientHandler;
import alluxio.security.authentication.Token;
import alluxio.security.capability.CapabilityToken;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

import javax.security.auth.callback.CallbackHandler;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;

/**
 * Creates {@link SaslClientHandler} instance for Delegation/Capability Tokens.
 */
public class SaslClientHandlerToken implements SaslClientHandler {
  private static final Logger LOG = LoggerFactory.getLogger(SaslClientHandlerToken.class);

  /** Underlying SaslClient. */
  private final SaslClient mSaslClient;

  /** Authentication scheme for given token. */
  private final ChannelAuthenticationScheme mAuthScheme;

  /**
   * Creates {@link SaslClientHandler} instance for Tokens.
   *
   * @param token a token
   * @param serverName server name
   * @throws UnauthenticatedException
   */
  public SaslClientHandlerToken(Token<?> token, String serverName)
      throws UnauthenticatedException {
    CallbackHandler cbHandler = new DigestClientCallbackHandler(token);
    mSaslClient = createSaslClient(cbHandler, serverName);
    // Only delegation/capability tokens are supported.
    if (token.getId() instanceof DelegationTokenIdentifier) {
      mAuthScheme = ChannelAuthenticationScheme.DELEGATION_TOKEN;
    } else if (token.getId() instanceof CapabilityToken.CapabilityTokenIdentifier) {
      mAuthScheme = ChannelAuthenticationScheme.CAPABILITY_TOKEN;
    } else {
      throw new RuntimeException(
          String.format("Unsupported token identifier found:%s", token.getId()));
    }
  }

  /**
   * Creates a {@link SaslClient} for token handling.
   *
   * @param cbHandler callback handler
   * @param serverName server name
   * @return the created client
   * @throws UnauthenticatedException
   */
  private SaslClient createSaslClient(CallbackHandler cbHandler, String serverName)
      throws UnauthenticatedException {
    try {
      return Sasl.createSaslClient(new String[] {TokenUtils.DIGEST_MECHANISM_NAME}, null,
          TokenUtils.TOKEN_PROTOCOL_NAME, serverName, new HashMap<String, String>(), cbHandler);
    } catch (SaslException se) {
      throw new UnauthenticatedException(se);
    }
  }

  @Override
  public void close() {
    if (mSaslClient != null) {
      try {
        mSaslClient.dispose();
      } catch (SaslException exc) {
        LOG.debug("Failed to close SaslClient.", exc);
      }
    }
  }

  @Override
  public ChannelAuthenticationScheme getClientScheme() {
    return mAuthScheme;
  }

  @Override
  public SaslClient getSaslClient() {
    return mSaslClient;
  }
}
