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

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;

/**
 * The capability is a token which grants the bearer specified access rights of an object to an
 * identity. It is used to achieve distributed authorization. In Alluxio, the file metadata,
 * including permission, is only stored in Master. The capability is a token granted by the Master,
 * and presented from the Alluxio client to the workers for block access.
 *
 * Note: The {@link CapabilityKey} should never be included in {@link Capability}.
 */
public final class Capability {
  private CapabilityContent mCapabilityContent;

  /** The authenticator for verifying capability is signed with an expected secret key. */
  private String mAuthenticator;

  /**
   * Constructor for {@link Capability}.
   *
   * @param key the capability key
   * @param content the capability content
   * @throws InvalidCapabilityException if the capability creation fails
   */
  public Capability(CapabilityKey key, CapabilityContent content)
      throws InvalidCapabilityException {
    mCapabilityContent = content;
    try {
      mAuthenticator = SecretManager.calculateHMAC(key.getEncodedKey(),
          mCapabilityContent.toString());
    } catch (SignatureException | NoSuchAlgorithmException | InvalidKeyException e) {
      throw new InvalidCapabilityException("Failed to create the capability authenticator", e);
    }
  }

  /**
   * @return the capability content
   */
  public CapabilityContent getCapabilityContent() {
    return mCapabilityContent;
  }

  /**
   * @return the authenticator
   */
  public String getAuthenticator() {
    return mAuthenticator;
  }

  private Capability() {} // prevent instantiation
}
