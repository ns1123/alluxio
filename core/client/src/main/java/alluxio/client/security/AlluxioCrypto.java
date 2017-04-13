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

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

/**
 * The Alluxio cryptographic library. It supports fetching crypto keys from KMS,
 * encrypting and decrypting data.
 */
public final class AlluxioCrypto {
  private static final String AES = "AES";
  private static final int AES_KEY_SIZE = 256; // in bits
  private static final int GCM_TAG_LENGTH = 16; // in bytes

  private Cipher mCipher;

  /**
   * Constructs a {@link AlluxioCrypto} with the given cipher.
   * @param cipher the cipher name
   * @throws NoSuchPaddingException if transformation contains a not available padding scheme
   * @throws NoSuchAlgorithmException if transformation is null, empty, in an invalid format
   * @throws NoSuchProviderException if no Provider supports a CipherSpi implementation
   */
  public AlluxioCrypto(String cipher) throws NoSuchPaddingException, NoSuchAlgorithmException,
      NoSuchProviderException {
    mCipher = Cipher.getInstance(cipher, "SunJCE");
  }

  /**
   * Gets a {@link CryptoKey} from the specified kms and input key.
   * @param kms the KMS endpoint
   * @param encrypt whether to encrypt or decrypt
   * @param inputKey the input key of the KMS calls
   * @return the fetched crypto key for encryption or decryption
   */
  public CryptoKey getCryptoKey(String kms, boolean encrypt, String inputKey) {
    // TODO(chaomin): integrate with KMS
    return new CryptoKey("AES256-GCM", "randomkey".getBytes(), "iv".getBytes(), false);
  }

  /**
   * Encrypts the input plaintext with the specified {@link CryptoKey}.
   * @param plaintext the input plaintext
   * @param cryptoKey the crypto key which contains the encryption key, iv and etc
   * @return the encrypted ciphertext
   * @throws BadPaddingException if the crypto library fails
   * @throws IllegalBlockSizeException if the crypto library fails
   * @throws InvalidAlgorithmParameterException if the parameter to crypto library is invalid
   * @throws InvalidKeyException if the crypto key is invalid
   * @throws NoSuchAlgorithmException if no Provider supports a MessageDigestSpi implementation
   *         for the specified algorithm
   */
  public byte[] encrypt(byte[] plaintext, CryptoKey cryptoKey) throws NoSuchAlgorithmException,
      InvalidAlgorithmParameterException, InvalidKeyException, BadPaddingException,
      IllegalBlockSizeException {
    byte[] key = cryptoKey.getKey();
    MessageDigest sha = MessageDigest.getInstance("SHA-1");
    key = sha.digest(key);
    key = Arrays.copyOf(key, AES_KEY_SIZE / 8); // use only first 256 bit
    SecretKeySpec secretKeySpec = new SecretKeySpec(key, AES);
    GCMParameterSpec spec = new GCMParameterSpec(GCM_TAG_LENGTH * 8, cryptoKey.getIv());
    mCipher.init(Cipher.ENCRYPT_MODE, secretKeySpec, spec);
    return mCipher.doFinal(plaintext);
  }

  /**
   * Decrypts the input ciphertext with the specified {@link CryptoKey}.
   * @param ciphertext the input ciphertext
   * @param cryptoKey the crypto key which contains the decryption key, iv, authTag and etc
   * @return the decrypted plaintext
   * @throws InvalidAlgorithmParameterException if the parameter to crypto library is invalid
   * @throws InvalidKeyException if the crypto key is invalid
   * @throws BadPaddingException if the crypto library fails
   * @throws IllegalBlockSizeException if the crypto library fails
   * @throws NoSuchAlgorithmException if no Provider supports a MessageDigestSpi implementation
   *         for the specified algorithm
   */
  public byte[] decrypt(byte[] ciphertext, CryptoKey cryptoKey)
      throws InvalidAlgorithmParameterException, InvalidKeyException, BadPaddingException,
      IllegalBlockSizeException, NoSuchAlgorithmException {
    byte[] key = cryptoKey.getKey();
    MessageDigest sha = MessageDigest.getInstance("SHA-1");
    key = sha.digest(key);
    key = Arrays.copyOf(key, AES_KEY_SIZE / 8); // use only first 256 bit
    SecretKeySpec secretKeySpec = new SecretKeySpec(key, AES);
    GCMParameterSpec spec = new GCMParameterSpec(GCM_TAG_LENGTH * 8, cryptoKey.getIv());
    mCipher.init(Cipher.DECRYPT_MODE, secretKeySpec, spec);
    return mCipher.doFinal(ciphertext);
  }
}
