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

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import org.apache.commons.codec.digest.HmacAlgorithms;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

import javax.annotation.concurrent.ThreadSafe;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/**
 * The capability key which contains the secret key which is shared between Master and Workers.
 * It has a key id representing the key version and an expiration time.
 */
@ThreadSafe
public final class CapabilityKey {
  private static final String HMAC_ALGORITHM = HmacAlgorithms.HMAC_SHA_1.toString();

  /** Key id for versioning. */
  private final long mKeyId;
  /** Key expiration time in millisecond. */
  private final long mExpirationTimeMs;
  /** The encoded secret key in bytes. */
  private final byte[] mEncodedKey;

  /** The Mac for calculation with current secret key. */
  // TODO(chaomin): make this thread local if synchronization on calculateHMAC is a bottleneck.
  private Mac mMac;

  /**
   * Default constructor.
   */
  public CapabilityKey() {
    mKeyId = 0L;
    mExpirationTimeMs = 0L;
    mEncodedKey = null;
  }

  /**
   * Creates a new {@link CapabilityKey} instance.
   *
   * @param keyId the capability key id
   * @param expirationTimeMs the expiration time in milliseconds
   * @param encodedKey the encoded key
   * @throws NoSuchAlgorithmException if the algorithm can not be found
   * @throws InvalidKeyException if the secret key is invalid
   */
  public CapabilityKey(long keyId, long expirationTimeMs, byte[] encodedKey)
      throws NoSuchAlgorithmException, InvalidKeyException {
    mKeyId = keyId;
    mExpirationTimeMs = expirationTimeMs;
    mEncodedKey = encodedKey == null ? null : Arrays.copyOf(encodedKey, encodedKey.length);

    if (encodedKey != null) {
      mMac = Mac.getInstance(HMAC_ALGORITHM);
      mMac.init(new SecretKeySpec(mEncodedKey, HMAC_ALGORITHM));
    }
  }

  /**
   * @return the key id
   */
  public long getKeyId() {
    return mKeyId;
  }

  /**
   * @return the expiration time
   */
  public long getExpirationTimeMs() {
    return mExpirationTimeMs;
  }

  /**
   * @return the encoded key
   */
  public byte[] getEncodedKey() {
    return mEncodedKey;
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

  /**
   * Calculates the HMAC for the data with current key.
   *
   * @param data the data input in bytes
   * @return the HMAC result in bytes
   */
  public byte[] calculateHMAC(byte[] data) {
    Preconditions.checkNotNull(mEncodedKey);
    Preconditions.checkNotNull(data);
    synchronized (mMac) {
      return mMac.doFinal(data);
    }
  }

  /**
   * Verifies whether the given capability is signed by the current key.
   *
   * @param capability the {@link Capability} to verify
   * @throws InvalidCapabilityException if the verification failed
   */
  public void verifyAuthenticator(Capability capability) throws InvalidCapabilityException {
    byte[] expectedAuthenticator = calculateHMAC(capability.getContent());
    if (!Arrays.equals(expectedAuthenticator, capability.getAuthenticator())) {
      // SECURITY: the expectedAuthenticator should never be printed in logs.
      throw new InvalidCapabilityException(
          "Invalid capability: the authenticator can not be verified.");
    }
  }
}
