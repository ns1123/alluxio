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

package alluxio.security.capability;

import alluxio.exception.InvalidCapabilityException;
import alluxio.util.CommonUtils;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;

/**
 * Util methods for Alluxio capability generation and verification.
 */
public final class CapabilityUtils {
  /**
   * Generates a capability with given key.
   *
   * @param key the capability key
   * @param content the capability content
   * @return the generated capability
   * @throws InvalidCapabilityException if fails to generate the capability
   */
  public static Capability generateCapability(CapabilityKey key, CapabilityContent content)
      throws InvalidCapabilityException {
    return new Capability(key, content);
  }

  /**
   * Verifies a capability with two possible keys.
   *
   * @param curKey the latest capability key
   * @param oldKey the old capability key
   * @param cap the capability to verify
   * @return true iff the capability is verified with the given keys
   * @throws InvalidCapabilityException if the capability ca not be verified with both keys
   */
  public static boolean verifyAuthenticator(CapabilityKey curKey, CapabilityKey oldKey,
      Capability cap) throws InvalidCapabilityException {
    if (curKey.getKeyId() != cap.getCapabilityContent().getKeyId()) {
      return verifyAuthenticator(oldKey, cap);
    } else {
      return verifyAuthenticator(curKey, cap);
    }
  }

  /**
   * Verifies a capability with given key.
   *
   * @param key the provided capability key
   * @param cap the capability to verify
   * @return true iff the capability is verified with the given key
   * @throws InvalidCapabilityException if the capability can not be verified
   */
  public static boolean verifyAuthenticator(CapabilityKey key, Capability cap)
      throws InvalidCapabilityException {
    if (cap.getCapabilityContent().getKeyId() != key.getKeyId()) {
      throw new InvalidCapabilityException("Invalid capability: the capability key id not match.");
    }
    if (CommonUtils.getCurrentMs() > cap.getCapabilityContent().getExpirationTimeMs()) {
      throw new InvalidCapabilityException("Capability expired, expiration time = "
          + cap.getCapabilityContent().getExpirationTimeMs());
    }
    try {
      String expectedAuthenticator = SecretManager.calculateHMAC(
          new String(key.getEncodedKey()), cap.getCapabilityContent().toString());
      if (!expectedAuthenticator.equals(cap.getAuthenticator())) {
        // SECURITY: the expectedAuthenticator should never be printed in logs.
        throw new InvalidCapabilityException(
            "Invalid capability: the authenticator can not be verified.");
      }
      return true;
    } catch (SignatureException | NoSuchAlgorithmException | InvalidKeyException e) {
      throw new InvalidCapabilityException("Failed to generate the authenticator with key", e);
    }
  }

  /**
   * Verifies a capability content with the requested fields.
   *
   * @param content the capability content
   * @param user the end user name
   * @param fileid the requested file id
   * @param accessMode the requested access mode
   * @return true iff the capability authorizes the requested fields
   * @throws InvalidCapabilityException if the capability can not grant the requested access
   */
  public static boolean verifyContent(CapabilityContent content, String user, long fileid,
      CapabilityContent.AccessMode accessMode) throws InvalidCapabilityException {
    if (!content.getOwner().equals(user)) {
      throw new InvalidCapabilityException("The authorized user " + content.getOwner()
          + " in capability does not match the end user " + user);
    }
    if (content.getFileId() != fileid) {
      throw new InvalidCapabilityException("The authorized fileid " + content.getFileId()
          + " in capability does not match the requested file id " + fileid);
    }
    if (!content.getAccessMode().includes(accessMode)) {
      throw new InvalidCapabilityException("The authorized access mode "
          + content.getAccessMode().toString() + " is not sufficient to do the requested access "
          + accessMode.toString());
    }
    return true;
  }

  private CapabilityUtils() {} // prevent instantiation
}
