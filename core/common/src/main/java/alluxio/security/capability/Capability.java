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
 * Note: The {@link CapabilityKey} should never be included in {@link Capability}.
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
   * Creates an instance of {@link Capability} from a {@link CapabilityKey} and a
   * {@link CapabilityProto.Content}.
   *
   * @param key the capability key
   * @param content the capability content
   */
  public Capability(CapabilityKey key, CapabilityProto.Content content) {
    mContentDecoded = content;
    mKeyId = key.getKeyId();
    mContent = ProtoUtils.encode(content);
    mAuthenticator = key.calculateHMAC(mContent);
  }

  /**
   * Creates an instance of {@link Capability} from a proto representation of the capability.
   *
   * @param capability the proto representation of the capability
   * @throws InvalidCapabilityException if the proto object is malformed
   */
  public Capability(alluxio.proto.security.CapabilityProto.Capability capability)
      throws InvalidCapabilityException {
    if (capability == null || !capability.hasContent() || !capability.hasAuthenticator()
        || !capability.hasKeyId()) {
      throw new InvalidCapabilityException(
          "Invalid thrift capability. The keyId, authenticator and content must be all set.");
    }

    mKeyId = capability.getKeyId();
    mAuthenticator = ProtoUtils.getAuthenticator(capability);
    mContent = ProtoUtils.getContent(capability);
  }

  /**
   * Creates an instance of {@link Capability} from a thrift representation of the capability.
   *
   * @param capability the thrift representation of the capability
   * @throws InvalidCapabilityException if the thrift object is malformed
   */
  public Capability(alluxio.thrift.Capability capability) throws InvalidCapabilityException {
    if (capability == null || !capability.isSetContent() || !capability.isSetAuthenticator()
        || !capability.isSetKeyId()) {
      throw new InvalidCapabilityException(
          "Invalid thrift capability. The keyId, authenticator and content must be all set.");
    }

    mKeyId = capability.getKeyId();
    mAuthenticator = capability.getAuthenticator();
    mContent = capability.getContent();
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
  public void verifyAuthenticator(CapabilityKey curKey, CapabilityKey oldKey)
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
  public void verifyAuthenticator(CapabilityKey key) throws InvalidCapabilityException {
    byte[] expectedAuthenticator = key.calculateHMAC(mContent);
    if (!Arrays.equals(expectedAuthenticator, mAuthenticator)) {
      // SECURITY: the expectedAuthenticator should never be printed in logs.
      throw new InvalidCapabilityException(
          "Invalid capability: the authenticator can not be verified.");
    }
  }

  /**
   * @return the thrift representation of the object
   */
  public alluxio.thrift.Capability toThrift() {
    alluxio.thrift.Capability capability = new alluxio.thrift.Capability();
    capability.setContent(mContent);
    capability.setAuthenticator(mAuthenticator);
    capability.setKeyId(mKeyId);
    return capability;
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
