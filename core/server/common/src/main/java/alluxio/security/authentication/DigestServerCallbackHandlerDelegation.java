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

import alluxio.security.authentication.token.DigestServerCallbackHandler;
import alluxio.security.authentication.token.TokenUtils;

import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.sasl.AuthorizeCallback;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * The delegation token sasl callback for the gRPC servers.
 */
class DigestServerCallbackHandlerDelegation extends DigestServerCallbackHandler {
  private static final Logger LOG = LoggerFactory.getLogger(DigestServerCallbackHandlerDelegation.class);

  private final DelegationTokenManager mTokenManager;
  private final SaslServerHandler mHandler;

  /**
   * Creates a {@link DigestServerCallbackHandlerDelegation} instance.
   *
   * @param manager delegation token manager
   */
  public DigestServerCallbackHandlerDelegation(SaslServerHandler handler,
      DelegationTokenManager manager) {
    mHandler = handler;
    mTokenManager = manager;
  }

  @Override
  protected void authorize(AuthorizeCallback ac) throws IOException {
    DelegationTokenIdentifier id = getDelegationTokenIdentifier(ac.getAuthorizationID());
    String authorizationId = id.getOwner();
    String authenticationId = id.getRealUser();
    ac.setAuthorized(true);
    ac.setAuthorizedID(authorizationId);
    mHandler.setAuthenticatedUserInfo(new AuthenticatedUserInfo(authorizationId, authenticationId,
        TokenUtils.DIGEST_MECHANISM_NAME));
  }

  @Override
  protected char[] getPassword(String name) {
    try {
      DelegationTokenIdentifier id = getDelegationTokenIdentifier(name);
      byte[] password = mTokenManager.retrievePassword(id);
      if (password == null) {
        LOG.debug("Token not found for id: {}", id);
        return new char[0];
      }
      return new String(Base64.encodeBase64(password), StandardCharsets.UTF_8).toCharArray();
    } catch (IOException e) {
      LOG.error(String.format("Cannot decode password for name: %s", name), e);
      return new char[0];
    }
  }

  private DelegationTokenIdentifier getDelegationTokenIdentifier(String name) throws IOException {
    return DelegationTokenIdentifier.fromByteArray(Base64.decodeBase64(name.getBytes()));
  }
}
