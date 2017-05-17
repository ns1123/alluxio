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

import java.security.GeneralSecurityException;

/**
 * Cipher using OpenSSL libcrypto with JNI.
 * It only supports AES/GCM/NoPadding for now.
 */
public final class OpenSSLCipher implements Cipher {
  private static final String AES_GCM_NOPADDING = "AES/GCM/NoPadding";

  private CryptoKey mCryptoKey;
  private OpMode mMode;

  @Override
  public void init(OpMode mode, CryptoKey cryptoKey) throws GeneralSecurityException {
    if (!mCryptoKey.getCipher().equals(AES_GCM_NOPADDING)) {
      throw new GeneralSecurityException("Unsupported cipher transformation");
    }
    mMode = mode;
    mCryptoKey = cryptoKey;
  }

  @Override
  public int doFinal(byte[] input, int inputOffset, int inputLen, byte[] output, int outputOffset)
      throws GeneralSecurityException {
    byte[] key = mCryptoKey.getKey();
    byte[] iv = mCryptoKey.getIv();
    switch (mMode) {
      case ENCRYPTION:
        return encrypt(input, inputOffset, inputLen, key, iv, output, outputOffset);
      case DECRYPTION:
        return decrypt(input, inputOffset, inputLen, key, iv, output, outputOffset);
      default:
        throw new GeneralSecurityException("Unknown operation mode");
    }
  }

  private native int encrypt(byte[] plaintext, int plaintextOffset, int plaintextLen, byte[] key,
      byte[] iv, byte[] ciphertext, int ciphertextOffset);

  private native int decrypt(byte[] ciphertext, int ciphertextOffset, int ciphertextLen, byte[] key,
      byte[] iv, byte[] plaintext, int plaintextOffset);
}
