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

package alluxio.client.security;

import java.util.Arrays;

/**
 * The cryptographic key object.
 */
public class CryptoKey {
  private String mCipher;
  private byte[] mKey;
  private byte[] mIv;
  private boolean mNeedsAuthTag;
  // For decryption only
  private byte[] mAuthTag;

  /**
   * Creates a new {@link CryptoKey}.
   * @param cipher the cipher name
   * @param key the cipher key
   * @param iv the initialization vector
   * @param needsAuthTag whether authTag is required or not
   */
  public CryptoKey(String cipher, byte[] key, byte[] iv, boolean needsAuthTag) {
    this(cipher, key, iv, needsAuthTag, null);
  }

  /**
   * Creates a new {@link CryptoKey} with an auth tag.
   * @param cipher the cipher name
   * @param key the cipher key
   * @param iv the initialization vector
   * @param needsAuthTag whether authTag is required or not
   * @param authTag the authentication tag
   */
  public CryptoKey(String cipher, byte[] key, byte[] iv, boolean needsAuthTag, byte[] authTag) {
    mCipher = cipher;
    mKey = key == null ? null : Arrays.copyOf(key, key.length);
    mIv = key == null ? null : Arrays.copyOf(iv, iv.length);
    mNeedsAuthTag = needsAuthTag;
    mAuthTag = authTag == null ? null : Arrays.copyOf(authTag, authTag.length);
  }

  /**
   * @return the cipher
   */
  public String getCipher() {
    return mCipher;
  }

  /**
   * @return the key
   */
  public byte[] getKey() {
    return mKey;
  }

  /**
   * @return the iv
   */
  public byte[] getIv() {
    return mIv;
  }

  /**
   * @return whether the auth tag is needed or not
   */
  public boolean isNeedsAuthTag() {
    return mNeedsAuthTag;
  }

  /**
   * @return the auth tag
   */
  public byte[] getAuthTag() {
    return mAuthTag;
  }
}
