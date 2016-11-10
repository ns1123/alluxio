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

import com.google.common.base.Objects;

import java.io.Serializable;
import java.util.Arrays;

import javax.crypto.SecretKey;

/**
 * The capability key which contains the secret key which is owned by Alluxio master and workers.
 * The capability key has a key id representing the key version and an expiration time.
 */
public final class CapabilityKey implements Serializable {
  private static final long serialVersionUID = -6513460762402075242L;

  /** Key id for versioning. */
  private long mKeyId;
  /** Key expiration time in millisecond. */
  private long mExpirationTimeMs;
  /** The encoded secret key in bytes. */
  private byte[] mEncodedKey;

  /**
   * @return the default {@link CapabilityKey}
   */
  public static CapabilityKey defaults() {
    return new CapabilityKey();
  }

  /**
   * Default constructor.
   */
  private CapabilityKey() {
    mKeyId = 0L;
    mExpirationTimeMs = 0L;
    mEncodedKey = null;
  }

  /**
   * @return the key id
   */
  public long getKeyId() {
    return mKeyId;
  }

  /**
   * @param keyId the key id to set
   * @return the updated object
   */
  public CapabilityKey setKeyId(long keyId) {
    mKeyId = keyId;
    return this;
  }

  /**
   * @return the expiration time
   */
  public long getExpirationTimeMs() {
    return mExpirationTimeMs;
  }

  /**
   * @param expirationTimeMs the expiration time to set in millisecond
   * @return the updated object
   */
  public CapabilityKey setExpirationTimeMs(long expirationTimeMs) {
    mExpirationTimeMs = expirationTimeMs;
    return this;
  }

  /**
   * @return the encoded key
   */
  public byte[] getEncodedKey() {
    return mEncodedKey;
  }

  /**
   * @param encodedKey the encoded key to set
   * @return the updated object
   */
  public CapabilityKey setEncodedKey(byte[] encodedKey) {
    mEncodedKey = encodedKey == null ? null : Arrays.copyOf(encodedKey, encodedKey.length);
    return this;
  }

  /**
   * Sets the key from {@link SecretKey}.
   *
   * @param key the secret key to set
   * @return the updated object
   */
  public CapabilityKey setEncodedKey(SecretKey key) {
    mEncodedKey = key == null ? null : key.getEncoded();
    return this;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mKeyId, mExpirationTimeMs, mEncodedKey);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof CapabilityKey)) {
      return false;
    }
    CapabilityKey that = (CapabilityKey) o;

    return mKeyId == that.getKeyId()
        && mExpirationTimeMs == that.getExpirationTimeMs()
        && Arrays.equals(mEncodedKey, that.getEncodedKey());
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("keyId", mKeyId)
        .add("expirationTimeMs", mExpirationTimeMs)
        .add("encodedKey", new String(mEncodedKey))
        .toString();
  }
}
