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
import java.security.MessageDigest;
import java.util.Arrays;

/**
 * Cipher using OpenSSL libcrypto with JNI.
 * It only supports AES/GCM/NoPadding for now.
 */
public final class OpenSSLCipher implements Cipher {
  private static final Logger LOG = LoggerFactory.getLogger(OpenSSLCipher.class);
  private static final String AES_GCM_NOPADDING = "AES/GCM/NoPadding";
  private static final String SHA1 = "SHA-1";
  private static final int AES_KEY_LENGTH = 16; // in bytes

  private byte[] mKey;
  private byte[] mIv;
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

  @Override
  public void init(OpMode mode, CryptoKey cryptoKey) throws GeneralSecurityException {
    if (!cryptoKey.getCipher().equals(AES_GCM_NOPADDING)) {
      throw new GeneralSecurityException("Unsupported cipher transformation");
    }
    mMode = mode;
    byte[] key = cryptoKey.getKey();
    MessageDigest sha = MessageDigest.getInstance(SHA1);
    key = sha.digest(key);
    mKey = Arrays.copyOf(key, AES_KEY_LENGTH); // use only first 16 bytes
    mIv = cryptoKey.getIv();
  }

  @Override
  public int doFinal(byte[] input, int inputOffset, int inputLen, byte[] output, int outputOffset)
      throws GeneralSecurityException {
    switch (mMode) {
      case ENCRYPTION:
        return encrypt(input, inputOffset, inputLen, mKey, mIv, output, outputOffset);
      case DECRYPTION:
        return decrypt(input, inputOffset, inputLen, mKey, mIv, output, outputOffset);
      default:
        throw new GeneralSecurityException("Unknown operation mode");
    }
  }

  private native int encrypt(byte[] plaintext, int plaintextOffset, int plaintextLen, byte[] key,
      byte[] iv, byte[] ciphertext, int ciphertextOffset);

  private native int decrypt(byte[] ciphertext, int ciphertextOffset, int ciphertextLen, byte[] key,
      byte[] iv, byte[] plaintext, int plaintextOffset);
}
