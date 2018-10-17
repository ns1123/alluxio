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

package alluxio.security;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.apache.commons.codec.digest.HmacAlgorithms;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

import javax.annotation.concurrent.ThreadSafe;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/**
 * The master key which contains the secret key which is shared between Master and Workers.
 * It has a key id representing the key version and an expiration time.
 */
@ThreadSafe
public final class MasterKey {
  private static final String HMAC_ALGORITHM = HmacAlgorithms.HMAC_SHA_1.toString();

  /** Key id for versioning. */
  private final long mKeyId;
  /** Key expiration time in millisecond. */
  private final long mExpirationTimeMs;
  /** The encoded secret key in bytes. */
  private final byte[] mEncodedKey;

  /** The Mac for calculation with current secret key. */
  private final Mac mMac;
  /** The thread local copy of Mac for calculation with current secret key. */
  private ThreadLocal<Mac> mThreadLocalMac = new ThreadLocal<>();

  /**
   * Default constructor.
   */
  public MasterKey() {
    mKeyId = 0L;
    mExpirationTimeMs = 0L;
    mEncodedKey = null;
    mMac = null;
  }

  /**
   * Creates a new {@link MasterKey} instance.
   *
   * @param keyId the master key id
   * @param expirationTimeMs the expiration time in milliseconds
   * @param encodedKey the encoded key
   * @throws NoSuchAlgorithmException if the algorithm can not be found
   * @throws InvalidKeyException if the secret key is invalid
   */
  public MasterKey(long keyId, long expirationTimeMs, byte[] encodedKey)
      throws NoSuchAlgorithmException, InvalidKeyException {
    mKeyId = keyId;
    mExpirationTimeMs = expirationTimeMs;
    mEncodedKey = encodedKey == null ? null : Arrays.copyOf(encodedKey, encodedKey.length);

    Mac mac = null;
    if (encodedKey != null) {
      mac = Mac.getInstance(HMAC_ALGORITHM);
      mac.init(new SecretKeySpec(mEncodedKey, HMAC_ALGORITHM));
    }
    mMac = mac;
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
    if (!(o instanceof MasterKey)) {
      return false;
    }
    MasterKey that = (MasterKey) o;

    return mKeyId == that.getKeyId()
        && mExpirationTimeMs == that.getExpirationTimeMs()
        && Arrays.equals(mEncodedKey, that.getEncodedKey());
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("keyId", mKeyId)
        .add("expirationTimeMs", mExpirationTimeMs)
        .add("encodedKey", mEncodedKey == null ? "" : new String(mEncodedKey))
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
    Mac mac = mThreadLocalMac.get();
    if (mac == null) {
      mac = cloneMac();
      mThreadLocalMac.set(mac);
    }
    return mac.doFinal(data);
  }

  /**
   * Clones the Mac.
   *
   * @return the cloned Mac object
   */
  private Mac cloneMac() {
    Preconditions.checkNotNull(mMac);
    try {
      return (Mac) mMac.clone();
    } catch (CloneNotSupportedException e) {
      // Should never happen.
      Throwables.propagate(e);
    }
    return null;
  }
}
