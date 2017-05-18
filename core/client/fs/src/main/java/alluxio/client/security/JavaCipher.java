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

import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

/**
 * Cipher using Java's native implementations.
 */
public final class JavaCipher implements Cipher {
  private static final String AES = "AES";
  private static final String SUN_JCE = "SunJCE";
  private static final int GCM_TAG_LENGTH = 16; // in bytes

  private javax.crypto.Cipher mCipher;

  /**
   * Creates a new {@link JavaCipher}, the cipher cannot be used until
   * {@link JavaCipher#init(OpMode, CryptoKey)} is called.
   */
  public JavaCipher() {}

  @Override
  public void init(OpMode mode, CryptoKey cryptoKey) throws GeneralSecurityException {
    int opMode = mode == OpMode.ENCRYPTION ? javax.crypto.Cipher.ENCRYPT_MODE :
        javax.crypto.Cipher.DECRYPT_MODE;
    SecretKeySpec secretKeySpec = new SecretKeySpec(cryptoKey.getKey(), AES);
    GCMParameterSpec paramSpec = new GCMParameterSpec(GCM_TAG_LENGTH * 8, cryptoKey.getIv());
    mCipher = javax.crypto.Cipher.getInstance(cryptoKey.getCipher(), SUN_JCE);
    mCipher.init(opMode, secretKeySpec, paramSpec);
  }

  @Override
  public int doFinal(byte[] input, int inputOffset, int inputLen, byte[] output, int outputOffset)
      throws GeneralSecurityException {
    return mCipher.doFinal(input, inputOffset, inputLen, output, outputOffset);
  }
}
