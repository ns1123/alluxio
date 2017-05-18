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

import alluxio.Configuration;
import alluxio.PropertyKey;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.GeneralSecurityException;

/**
 * Cipher using OpenSSL libcrypto with JNI.
 * It only supports AES/GCM/NoPadding for now, the key length should be 128, 192, or 256 bits.
 */
public final class OpenSSLCipher implements Cipher {
  private static final Logger LOG = LoggerFactory.getLogger(OpenSSLCipher.class);
  private static final String AES_GCM_NOPADDING = "AES/GCM/NoPadding";

  private CryptoKey mCryptoKey;
  private OpMode mMode;

  static {
    String lib = Configuration.get(PropertyKey.SECURITY_ENCRYPTION_OPENSSL_LIB);
    if (lib.isEmpty()) {
      throw new RuntimeException(PropertyKey.Name.SECURITY_ENCRYPTION_OPENSSL_LIB + " is not set");
    }
    LOG.info("Loading Alluxio native library liballuxio from " + lib);
    System.load(lib);
    LOG.info("liballuxio was loaded");
  }

  /**
   * Creates a new {@link OpenSSLCipher}, the cipher cannot be used until
   * {@link OpenSSLCipher#init(OpMode, CryptoKey)} is called.
   */
  public OpenSSLCipher() {}

  @Override
  public void init(OpMode mode, CryptoKey cryptoKey) throws GeneralSecurityException {
    if (!cryptoKey.getCipher().equals(AES_GCM_NOPADDING)) {
      throw new GeneralSecurityException("Unsupported cipher transformation");
    }
    mCryptoKey = cryptoKey;
    mMode = mode;
  }

  @Override
  public int doFinal(byte[] input, int inputOffset, int inputLen, byte[] output, int outputOffset)
      throws GeneralSecurityException {
    switch (mMode) {
      case ENCRYPTION:
        return encrypt(input, inputOffset, inputLen, mCryptoKey.getKey(), mCryptoKey.getIv(),
            output, outputOffset);
      case DECRYPTION:
        return decrypt(input, inputOffset, inputLen, mCryptoKey.getKey(), mCryptoKey.getIv(),
            output, outputOffset);
      default:
        throw new GeneralSecurityException("Unknown operation mode");
    }
  }

  private native int encrypt(byte[] plaintext, int plaintextOffset, int plaintextLen, byte[] key,
      byte[] iv, byte[] ciphertext, int ciphertextOffset);

  private native int decrypt(byte[] ciphertext, int ciphertextOffset, int ciphertextLen, byte[] key,
      byte[] iv, byte[] plaintext, int plaintextOffset);
}
