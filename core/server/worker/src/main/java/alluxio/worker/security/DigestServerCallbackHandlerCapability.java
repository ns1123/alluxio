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

import alluxio.proto.security.CapabilityProto;
import alluxio.security.MasterKey;
import alluxio.security.authentication.AuthenticatedUserInfo;
import alluxio.security.authentication.SaslServerHandler;
import alluxio.security.authentication.token.DigestServerCallbackHandler;
import alluxio.security.authentication.token.TokenUtils;
import alluxio.util.proto.ProtoUtils;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.sasl.AuthorizeCallback;
import java.io.IOException;
import java.util.List;

/**
 * The capability token sasl callback for the gRPC servers.
 */
public class DigestServerCallbackHandlerCapability extends DigestServerCallbackHandler {
  private static final Logger LOG =
      LoggerFactory.getLogger(DigestServerCallbackHandlerCapability.class);

  /** Underlying Sasl handler. */
  private SaslServerHandler mHandler;
  /** List of active master keys. */
  private List<MasterKey> mActiveMasterKeys;

  /**
   * @param handler owning {@link SaslServerHandler} instance
   * @param activeMasterKeys list of active master keys that are used for signing capabilities
   */
  public DigestServerCallbackHandlerCapability(SaslServerHandler handler,
                                               List<MasterKey> activeMasterKeys) {
    mHandler = handler;
    mActiveMasterKeys = activeMasterKeys;
  }

  @Override
  protected void authorize(AuthorizeCallback ac) throws IOException {
    ac.setAuthorized(true);
    // Extract from authorized user, which is a capability content, the username
    // and save it in the channel for next handlers.
    byte[] contentProto = Base64.decodeBase64(ac.getAuthorizedID());
    CapabilityProto.Content contentDec = ProtoUtils.decode(contentProto);
    String capabilityUser = contentDec.getUser();
    // Update the handler with the authenticated user info.
    mHandler.setAuthenticatedUserInfo(new AuthenticatedUserInfo(capabilityUser, capabilityUser,
        TokenUtils.DIGEST_MECHANISM_NAME));
  }

  @Override
  protected char[] getPassword(String name) {
    try {
      // Extract keyId from the user word that is being authenticated
      byte[] contentProto = Base64.decodeBase64(name);
      CapabilityProto.Content contentDec = ProtoUtils.decode(contentProto);
      long keyId = contentDec.getKeyId();
      MasterKey clientMasterKey = null;
      for (MasterKey serverKey : mActiveMasterKeys) {
        if (serverKey.getKeyId() == keyId) {
          LOG.debug("Found a master key for received client content with Id:{}", keyId);
          clientMasterKey = serverKey;
          break;
        }
      }
      Preconditions.checkNotNull(clientMasterKey,
          "Failed to find master key:{} among active server keys.", keyId);

      byte[] authenticator = clientMasterKey.calculateHMAC(contentProto);
      String strAuthenticatorEnc =
          new String(Base64.encodeBase64(authenticator, false), Charsets.UTF_8);
      return strAuthenticatorEnc.toCharArray();
    } catch (Exception e) {
      LOG.error("Cannot obtain password", e);
      return new char[0];
    }
  }
}
