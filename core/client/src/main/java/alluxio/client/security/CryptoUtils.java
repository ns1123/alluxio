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

import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.util.Arrays;

import javax.annotation.concurrent.ThreadSafe;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

/**
 * The Alluxio cryptographic utilities. It supports fetching crypto keys from KMS,
 * encrypting and decrypting data.
 */
@ThreadSafe
public final class CryptoUtils {
  private static final String AES = "AES";
  private static final String SHA1 = "SHA-1";
  private static final int AES_KEY_LENGTH = 16; // in bytes
  private static final int GCM_TAG_LENGTH = 16; // in bytes

  /**
   * Gets a {@link CryptoKey} from the specified kms and input key.
   *
   * @param kms the KMS endpoint
   * @param encrypt whether to encrypt or decrypt
   * @param inputKey the input key of the KMS calls
   * @return the fetched crypto key for encryption or decryption
   */
  public static CryptoKey getCryptoKey(String kms, boolean encrypt, String inputKey) {
    // TODO(chaomin): integrate with KMS
    return new CryptoKey("AES256-GCM", "randomkey".getBytes(), "iv".getBytes(), false);
  }

  /**
   * Encrypts the input plaintext with the specified {@link CryptoKey}.
   *
   * @param plaintext the input plaintext
   * @param cryptoKey the crypto key which contains the encryption key, iv and etc
   * @return the encrypted ciphertext
   */
  public static byte[] encrypt(byte[] plaintext, CryptoKey cryptoKey) {
    try {
      Cipher cipher = Cipher.getInstance(cryptoKey.getCipher(), "SunJCE");
      byte[] key = cryptoKey.getKey();
      MessageDigest sha = MessageDigest.getInstance(SHA1);
      key = sha.digest(key);
      key = Arrays.copyOf(key, AES_KEY_LENGTH); // use only first 16 bytes
      SecretKeySpec secretKeySpec = new SecretKeySpec(key, AES);
      GCMParameterSpec spec = new GCMParameterSpec(GCM_TAG_LENGTH * 8, cryptoKey.getIv());
      cipher.init(Cipher.ENCRYPT_MODE, secretKeySpec, spec);
      return cipher.doFinal(plaintext);
    } catch (BadPaddingException | InvalidAlgorithmParameterException | IllegalBlockSizeException
        | InvalidKeyException | NoSuchAlgorithmException | NoSuchPaddingException
        | NoSuchProviderException e) {
      throw new RuntimeException("Failed to encrypt the plaintext with given key ", e);
    }
  }

  /**
   * Decrypts the input ciphertext with the specified {@link CryptoKey}.
   *
   * @param ciphertext the input ciphertext
   * @param cryptoKey the crypto key which contains the decryption key, iv, authTag and etc
   * @return the decrypted plaintext
   */
  public static byte[] decrypt(byte[] ciphertext, CryptoKey cryptoKey) {
    try {
      Cipher cipher = Cipher.getInstance(cryptoKey.getCipher(), "SunJCE");
      byte[] key = cryptoKey.getKey();
      MessageDigest sha = MessageDigest.getInstance(SHA1);
      key = sha.digest(key);
      key = Arrays.copyOf(key, AES_KEY_LENGTH); // use only first 16 bytes
      SecretKeySpec secretKeySpec = new SecretKeySpec(key, AES);
      GCMParameterSpec spec = new GCMParameterSpec(GCM_TAG_LENGTH * 8, cryptoKey.getIv());
      cipher.init(Cipher.DECRYPT_MODE, secretKeySpec, spec);
      return cipher.doFinal(ciphertext);
    } catch (BadPaddingException | InvalidAlgorithmParameterException | IllegalBlockSizeException
        | InvalidKeyException | NoSuchAlgorithmException | NoSuchPaddingException
        | NoSuchProviderException e) {
      throw new RuntimeException("Failed to decrypt the plaintext with given key ", e);
    }
  }

  private CryptoUtils() {} // prevent instantiation
}
