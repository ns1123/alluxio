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
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.proto.security.EncryptionProto;
import alluxio.util.JNIUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;
import java.security.GeneralSecurityException;

/**
 * Cipher using OpenSSL libcrypto with JNI.
 * It only supports AES/GCM/NoPadding for now, the key length should be 128, 192, or 256 bits.
 */
// TODO(cc): Support ciphers other than AES/GCM/NoPadding.
public final class OpenSSLCipher implements Cipher {
  private static final Logger LOG = LoggerFactory.getLogger(OpenSSLCipher.class);
  private static final String AES_GCM_NOPADDING = Constants.AES_GCM_NOPADDING;

  private EncryptionProto.CryptoKey mCryptoKey;
  private OpMode mMode;

  static {
    String libDir = Configuration.get(PropertyKey.NATIVE_LIBRARY_PATH);
    String name = System.mapLibraryName(Constants.NATIVE_ALLUXIO_LIB_NAME);
    String libFile = Paths.get(libDir, name).toString();
    JNIUtils.load(LOG, libFile);
  }

  /**
   * Creates a new {@link OpenSSLCipher}.
   *
   * @param mode the operation mode
   * @param cryptoKey the cipher parameters
   */
  public OpenSSLCipher(OpMode mode, EncryptionProto.CryptoKey cryptoKey)
      throws GeneralSecurityException {
    String cipherName = cryptoKey.getCipher();
    // This is special handling for TS KMS, because it returns the cipher name : id-aes128-GCM
    if (cipherName.contains("aes128-GCM") || cipherName.contains("aes256-GCM")) {
      cipherName = Constants.AES_GCM_NOPADDING;
    }
    if (!cipherName.equals(AES_GCM_NOPADDING)) {
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
        return encrypt(input, inputOffset, inputLen, mCryptoKey.getKey().toByteArray(),
            mCryptoKey.getIv().toByteArray(), output, outputOffset);
      case DECRYPTION:
        return decrypt(input, inputOffset, inputLen, mCryptoKey.getKey().toByteArray(),
            mCryptoKey.getIv().toByteArray(), output, outputOffset);
      default:
        throw new GeneralSecurityException("Unknown operation mode");
    }
  }

  private native int encrypt(byte[] plaintext, int plaintextOffset, int plaintextLen, byte[] key,
      byte[] iv, byte[] ciphertext, int ciphertextOffset);

  private native int decrypt(byte[] ciphertext, int ciphertextOffset, int ciphertextLen, byte[] key,
      byte[] iv, byte[] plaintext, int plaintextOffset);
}
