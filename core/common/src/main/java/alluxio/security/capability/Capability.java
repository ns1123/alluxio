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
import alluxio.proto.security.CapabilityProto;
import alluxio.security.MasterKey;
import alluxio.util.proto.ProtoUtils;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Objects;

import java.io.IOException;
import java.util.Arrays;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * The capability is a token which grants the bearer specified access rights of an object to an
 * identity. It is used to achieve distributed authorization. In Alluxio, the file metadata,
 * including permission, is only stored in Master. The capability is a token granted by the Master,
 * and presented from the Alluxio client to the workers for block access.
 *
 * Note: The {@link MasterKey} should never be included in {@link Capability}.
 */
@NotThreadSafe
public final class Capability {
  private final byte[] mContent;
  /** The authenticator in bytes for verifying capability is signed with an expected secret key. */
  private final byte[] mAuthenticator;
  /** The key Id derived from the capability key for versioning. */
  private final Long mKeyId;

  /** The capability content decoded from mContent. */
  private CapabilityProto.Content mContentDecoded = null;

  /**
   * Creates an instance of {@link Capability} from a {@link MasterKey} and a
   * {@link CapabilityProto.Content}.
   *
   * @param key the capability key
   * @param content the capability content
   */
  public Capability(MasterKey key, CapabilityProto.Content content) {
    // TODO(feng): refactor Capability to unify with Token
    mContentDecoded = content;
    mKeyId = key.getKeyId();
    // Augment given content with the key Id
    // PS: No need to augment decoded content as it's used only to initialize this class
    CapabilityProto.Content augmentedContent =
        CapabilityProto.Content.newBuilder(content).setKeyId(mKeyId).build();
    mContent = ProtoUtils.encode(augmentedContent);
    mAuthenticator = key.calculateHMAC(mContent);
  }

  /**
   * Creates an instance of {@link Capability} from a proto representation of the capability.
   *
   * @param capability the proto representation of the capability
   * @throws InvalidCapabilityException if the proto object is malformed
   */
  public Capability(CapabilityProto.Capability capability)
      throws InvalidCapabilityException {
    if (capability == null || !capability.hasContent() || !capability.hasAuthenticator()
        || !capability.hasKeyId()) {
      throw new InvalidCapabilityException(
          "Invalid proto capability. The keyId, authenticator and content must be all set.");
    }

    mKeyId = capability.getKeyId();
    mAuthenticator = ProtoUtils.getAuthenticator(capability);
    // Augment given content with the key Id
    try {
      CapabilityProto.Content augmentedContent =
          CapabilityProto.Content.newBuilder().mergeFrom(ProtoUtils.getContent(capability))
              .setKeyId(mKeyId).build();
      mContent = ProtoUtils.encode(augmentedContent);
    } catch (Exception exc) {
      throw new InvalidCapabilityException("Content buffer is not valid :"
              + capability.toString()
              + "Exception :" + exc.getMessage());
    }
  }

  /**
   * @return the capability content
   */
  public byte[] getContent() {
    return mContent;
  }

  /**
   * @return the authenticator in bytes
   */
  public byte[] getAuthenticator() {
    return mAuthenticator;
  }

  /**
   * @return the content in proto
   * @throws InvalidCapabilityException if it fails to decode the capability content
   */
  @JsonIgnore
  public CapabilityProto.Content getContentDecoded() throws InvalidCapabilityException {
    if (mContentDecoded != null) {
      return mContentDecoded;
    }
    if (mContent == null) {
      return null;
    }
    try {
      mContentDecoded = ProtoUtils.decode(mContent);
    } catch (IOException e) {
      throw new InvalidCapabilityException("Failed to decode the capability content", e);
    }
    return mContentDecoded;
  }

  /**
   * @return the key Id
   */
  public long getKeyId() {
    return mKeyId;
  }

  /**
   * Verifies a capability with two possible keys.
   *
   * @param curKey the latest capability key
   * @param oldKey the old capability key
   * @throws InvalidCapabilityException if the capability can not be verified with either key
   */
  public void verifyAuthenticator(MasterKey curKey, MasterKey oldKey)
      throws InvalidCapabilityException {
    if (curKey.getKeyId() != mKeyId) {
      verifyAuthenticator(oldKey);
    } else {
      verifyAuthenticator(curKey);
    }
  }

  /**
   * Verifies a capability with given key.
   *
   * @param key the provided capability key
   * @throws InvalidCapabilityException if the capability can not be verified
   */
  public void verifyAuthenticator(MasterKey key) throws InvalidCapabilityException {
    byte[] expectedAuthenticator = key.calculateHMAC(mContent);
    if (!Arrays.equals(expectedAuthenticator, mAuthenticator)) {
      // SECURITY: the expectedAuthenticator should never be printed in logs.
      throw new InvalidCapabilityException(
          "Invalid capability: the authenticator can not be verified.");
    }
  }

  /**
   * @return the proto representation of the object
   */
  public alluxio.proto.security.CapabilityProto.Capability toProto() {
    CapabilityProto.Capability.Builder builder = CapabilityProto.Capability.newBuilder();
    ProtoUtils.setContent(builder, mContent);
    ProtoUtils.setAuthenticator(builder, mAuthenticator);
    return builder.setKeyId(mKeyId).build();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Capability)) {
      return false;
    }
    Capability other = (Capability) o;
    return Arrays.equals(mContent, other.mContent) && Arrays
        .equals(mAuthenticator, other.mAuthenticator) && Objects.equal(mKeyId, other.mKeyId);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(Arrays.hashCode(mContent), Arrays.hashCode(mAuthenticator), mKeyId);
  }

  private Capability() {
    mContent = null;
    mAuthenticator = null;
    mKeyId = null;
  } // required for JSON deserialization and equality testing
}
