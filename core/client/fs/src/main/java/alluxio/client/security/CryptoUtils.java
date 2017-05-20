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

import alluxio.client.LayoutSpec;
import alluxio.client.LayoutUtils;
import alluxio.network.protocol.databuffer.DataBuffer;

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The Alluxio cryptographic utilities. It supports fetching crypto keys from KMS,
 * encrypting and decrypting data.
 */
@ThreadSafe
public final class CryptoUtils {
  private static final String SHA1 = "SHA-1";
  private static final int AES_KEY_LENGTH = 16; // in bytes

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
   * Encrypts the input plaintext with the specified {@link CryptoKey} into the ciphertext
   * with given offset and length.
   *
   * @param cryptoKey the crypto key which contains the encryption key, iv and etc
   * @param plaintext the input plaintext
   * @param inputOffset the offset in plaintext where the input starts
   * @param inputLen the input length to encrypt
   * @param ciphertext the encrypted ciphertext to write to
   * @param outputOffset the offset in ciphertext where the result is stored
   * @return the number of bytes stored in ciphertext
   */
  public static int encrypt(CryptoKey cryptoKey, byte[] plaintext, int inputOffset, int inputLen,
                            byte[] ciphertext, int outputOffset) {
    try {
      Cipher cipher = Cipher.Factory.create(Cipher.OpMode.ENCRYPTION, toAES128Key(cryptoKey));
      return cipher.doFinal(plaintext, inputOffset, inputLen, ciphertext, outputOffset);
    } catch (GeneralSecurityException e) {
      throw new RuntimeException("Failed to encrypt the plaintext with given key ", e);
    }
  }

  /**
   * Encrypts the input ByteBuf at chunk level and return the ciphertext in another ByteBuf.
   * It takes the ownership of the input ByteBuf.
   *
   * @param spec the layout spec with chunk layout sizes
   * @param cryptoKey the crypto key which contains the decryption key, iv, authTag and etc
   * @param input the input plaintext in a ByteBuf
   * @return the encrypted content in a ByteBuf
   */
  public static ByteBuf encryptChunks(LayoutSpec spec, CryptoKey cryptoKey, ByteBuf input) {
    final int chunkSize = (int) spec.getChunkSize();
    final int chunkFooterSize = (int) spec.getChunkFooterSize();
    int logicalTotalLen = input.readableBytes();
    int physicalTotalLen = (int) LayoutUtils.toPhysicalLength(spec, 0, logicalTotalLen);
    byte[] ciphertext = new byte[physicalTotalLen];
    byte[] plainChunk = new byte[chunkSize];
    try {
      int logicalPos = 0;
      int physicalPos = 0;
      cryptoKey = toAES128Key(cryptoKey);
      while (logicalPos < logicalTotalLen) {
        int logicalLeft = logicalTotalLen - logicalPos;
        int logicalChunkLen = Math.min(chunkSize, logicalLeft);
        input.getBytes(logicalPos, plainChunk, 0 /* dest index */, logicalChunkLen /* len */);
        Cipher cipher = Cipher.Factory.create(Cipher.OpMode.ENCRYPTION, cryptoKey);
        int physicalChunkLen =
            cipher.doFinal(plainChunk, 0, logicalChunkLen, ciphertext, physicalPos);
        Preconditions.checkState(physicalChunkLen == logicalChunkLen + chunkFooterSize);
        logicalPos += logicalChunkLen;
        physicalPos += physicalChunkLen;
      }
      Preconditions.checkState(physicalPos == physicalTotalLen);
      // TODO(chaomin): avoid heavy use of Unpooled buffer.
      return Unpooled.wrappedBuffer(ciphertext);
    } catch (GeneralSecurityException e) {
      throw new RuntimeException("Failed to encrypt the plaintext with given key ", e);
    } finally {
      input.release();
    }
  }

  /**
   * Decrypts the input ciphertext with the specified {@link CryptoKey}.
   *
   * @param ciphertext the input ciphertext
   * @param cryptoKey the crypto key which contains the decryption key, iv, authTag and etc
   * @param inputOffset the offset in plaintext where the input starts
   * @param inputLen the input length to decrypt
   * @param plaintext the decrypted plaintext to write to
   * @param outputOffset the offset in plaintext where the result is stored
   * @return the number of bytes stored in plaintext
   */
  public static int decrypt(CryptoKey cryptoKey, byte[] ciphertext, int inputOffset, int inputLen,
                            byte[] plaintext, int outputOffset) {
    try {
      Cipher cipher = Cipher.Factory.create(Cipher.OpMode.DECRYPTION, toAES128Key(cryptoKey));
      return cipher.doFinal(ciphertext, inputOffset, inputLen, plaintext, outputOffset);
    } catch (GeneralSecurityException e) {
      throw new RuntimeException("Failed to decrypt the ciphertext with given key ", e);
    }
  }

  /**
   * Decrypts the input ByteBuf at chunk level and return the plaintext in another DataBuffer.
   * It takes the ownership of the input DataBuffer.
   *
   * @param spec the layout spec with chunk layout sizes
   * @param cryptoKey the crypto key which contains the decryption key, iv, authTag and etc
   * @param input the input ciphertext in a DataBuffer
   * @return the decrypted content in a byte array
   */
  public static byte[] decryptChunks(LayoutSpec spec, CryptoKey cryptoKey, DataBuffer input) {
    final int chunkFooterSize = (int) spec.getChunkFooterSize();
    final int physicalChunkSize = (int) spec.getPhysicalChunkSize();

    // Physical chunk size is either a full physical chunk, or the last chunk to EOF.
    int physicalTotalLen = input.readableBytes();
    int logicalTotalLen = (int) LayoutUtils.toLogicalLength(spec, 0, physicalTotalLen);
    byte[] plaintext = new byte[logicalTotalLen];
    byte[] cipherChunk = new byte[physicalChunkSize];
    try {
      int logicalPos = 0;
      int physicalPos = 0;
      cryptoKey = toAES128Key(cryptoKey);
      while (physicalPos < physicalTotalLen) {
        int physicalLeft = physicalTotalLen - physicalPos;
        int physicalChunkLen = Math.min(physicalChunkSize, physicalLeft);
        input.readBytes(cipherChunk, 0 /* dest chunk */, physicalChunkLen /* len */);
        // Decryption always stops at the chunk boundary, with either a full chunk or the last
        // chunk till the end of the block.
        Cipher cipher = Cipher.Factory.create(Cipher.OpMode.DECRYPTION, cryptoKey);
        int logicalChunkLen =
            cipher.doFinal(cipherChunk, 0, physicalChunkLen, plaintext, logicalPos);
        Preconditions.checkState(logicalChunkLen + chunkFooterSize == physicalChunkLen);
        logicalPos += logicalChunkLen;
        physicalPos += physicalChunkLen;
      }
      Preconditions.checkState(logicalPos == logicalTotalLen);
      return plaintext;
    } catch (GeneralSecurityException e) {
      throw new RuntimeException("Failed to decrypt the ciphertext with given key ", e);
    } finally {
      input.release();
    }
  }

  // TODO(cc): this method is to ensure the key is 128 bits, remove this once KMS is available.
  private static CryptoKey toAES128Key(CryptoKey key) throws NoSuchAlgorithmException {
    MessageDigest sha = MessageDigest.getInstance(SHA1);
    byte[] newKey = Arrays.copyOf(sha.digest(key.getKey()), AES_KEY_LENGTH); // use only first 16 bytes
    return new CryptoKey(key.getCipher(), newKey, key.getIv(), key.isNeedsAuthTag(),
        key.getAuthTag());
  }

  private CryptoUtils() {} // prevent instantiation
}
