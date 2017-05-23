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
import alluxio.proto.security.EncryptionProto;

import java.security.GeneralSecurityException;

/**
 * Interface for all ciphers.
 */
public interface Cipher {
  /**
   * Operation mode of the cipher.
   */
  enum OpMode {
    ENCRYPTION,
    DECRYPTION,
  }

  /**
   * Encrypts or decrypts the first inputLen bytes in the input buffer, starting at inputOffset
   * inclusive. The result is stored in the output buffer, starting at outputOffset inclusive.
   *
   * @param input the input buffer
   * @param inputOffset the offset in input where the input starts
   * @param inputLen the input length
   * @param output the output buffer
   * @param outputOffset the offset in output where the result is stored
   * @return the number of bytes stored in output
   * @throws GeneralSecurityException if the encryption or decryption fails
   */
  int doFinal(byte[] input, int inputOffset, int inputLen, byte[] output, int outputOffset)
      throws GeneralSecurityException;

  /**
   * Factory for creating ciphers based on the configuration.
   */
  final class Factory {
    private Factory() {} // prevent instantiation

    /**
     * @return a {@link JavaCipher} or an {@link OpenSSLCipher} based on configuration
     * @param mode the operation mode
     * @param cryptoKey the cipher parameters
     */
    public static Cipher create(OpMode mode, EncryptionProto.CryptoKey cryptoKey)
        throws GeneralSecurityException {
      if (Configuration.getBoolean(PropertyKey.SECURITY_ENCRYPTION_OPENSSL_ENABLED)) {
        return new OpenSSLCipher(mode, cryptoKey);
      }
      return new JavaCipher(mode, cryptoKey);
    }
  }
}
