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
import alluxio.client.LayoutSpec;
import alluxio.client.LayoutUtils;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.network.protocol.databuffer.DataByteBuffer;
import alluxio.network.protocol.databuffer.DataNettyBufferV2;

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.nio.ByteBuffer;
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
import javax.crypto.ShortBufferException;
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
  private static final String SUN_JCE = "SunJCE";
  private static final int AES_KEY_LENGTH = 16; // in bytes
  private static final int GCM_TAG_LENGTH = 16; // in bytes

  private static final LayoutSpec SPEC = LayoutSpec.Factory.createFromConfiguration();

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
      Cipher cipher = Cipher.getInstance(cryptoKey.getCipher(), SUN_JCE);
      byte[] key = cryptoKey.getKey();
      MessageDigest sha = MessageDigest.getInstance(SHA1);
      key = sha.digest(key);
      byte[] sizedKey = Arrays.copyOf(key, AES_KEY_LENGTH); // use only first 16 bytes
      SecretKeySpec secretKeySpec = new SecretKeySpec(sizedKey, AES);
      GCMParameterSpec spec = new GCMParameterSpec(GCM_TAG_LENGTH * 8, cryptoKey.getIv());
      cipher.init(Cipher.ENCRYPT_MODE, secretKeySpec, spec);
      return cipher.doFinal(plaintext, inputOffset, inputLen, ciphertext, outputOffset);
    } catch (BadPaddingException | InvalidAlgorithmParameterException | IllegalBlockSizeException
        | InvalidKeyException | NoSuchAlgorithmException | NoSuchPaddingException
        | NoSuchProviderException | ShortBufferException e) {
      throw new RuntimeException("Failed to encrypt the plaintext with given key ", e);
    }
  }

  /**
   * Encrypts the input ByteBuf and return the ciphertext in another ByteBuf. This takes the
   * ownership of the input ByteBuf.
   *
   * @param cryptoKey the crypto key which contains the decryption key, iv, authTag and etc
   * @param input the input plaintext in a ByteBuf
   * @return the encrypted content in a ByteBuf
   */
  public static ByteBuf encrypt(CryptoKey cryptoKey, ByteBuf input) {
    final int chunkSize =
        (int) Configuration.getBytes(PropertyKey.USER_ENCRYPTION_CHUNK_SIZE_BYTES);
    final long chunkFooterSize =
        Configuration.getBytes(PropertyKey.USER_ENCRYPTION_CHUNK_FOOTER_SIZE_BYTES);
    int logicalTotalLen = input.readableBytes();
    int physicalTotalLen = (int) LayoutUtils.toPhysicalLength(SPEC, 0, logicalTotalLen);
    byte[] ciphertext = new byte[physicalTotalLen];
    byte[] plainChunk = new byte[chunkSize];
    try {
      byte[] key = cryptoKey.getKey();
      MessageDigest sha = MessageDigest.getInstance(SHA1);
      key = sha.digest(key);
      byte[] encryptKey = Arrays.copyOf(key, AES_KEY_LENGTH); // use only first 16 bytes
      SecretKeySpec secretKeySpec = new SecretKeySpec(encryptKey, AES);
      GCMParameterSpec spec = new GCMParameterSpec(GCM_TAG_LENGTH * 8, cryptoKey.getIv());

      int logicalPos = 0;
      int physicalPos = 0;
      while (logicalPos < logicalTotalLen) {
        int logicalLeft = logicalTotalLen - logicalPos;
        int logicalChunkLen = Math.min(chunkSize, logicalLeft);
        input.getBytes(logicalPos, plainChunk, 0 /* dest index */, logicalChunkLen /* len */);
        Cipher cipher = Cipher.getInstance(cryptoKey.getCipher(), "SunJCE");
        cipher.init(Cipher.ENCRYPT_MODE, secretKeySpec, spec);
        int physicalChunkLen =
            cipher.doFinal(plainChunk, 0, logicalChunkLen, ciphertext, physicalPos);
        Preconditions.checkState(physicalChunkLen == logicalChunkLen + chunkFooterSize);
        logicalPos += logicalChunkLen;
        physicalPos += physicalChunkLen;
      }
      Preconditions.checkState(physicalPos == physicalTotalLen);
      return Unpooled.wrappedBuffer(ciphertext);
    } catch (BadPaddingException | InvalidAlgorithmParameterException | IllegalBlockSizeException
        | InvalidKeyException | NoSuchAlgorithmException | NoSuchPaddingException
        | NoSuchProviderException | ShortBufferException e) {
      throw new RuntimeException("Failed to decrypt the plaintext with given key ", e);
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
      Cipher cipher = Cipher.getInstance(cryptoKey.getCipher(), "SunJCE");
      byte[] key = cryptoKey.getKey();
      MessageDigest sha = MessageDigest.getInstance(SHA1);
      key = sha.digest(key);
      byte[] sizedKey = Arrays.copyOf(key, AES_KEY_LENGTH); // use only first 16 bytes
      SecretKeySpec secretKeySpec = new SecretKeySpec(sizedKey, AES);
      GCMParameterSpec spec = new GCMParameterSpec(GCM_TAG_LENGTH * 8, cryptoKey.getIv());
      cipher.init(Cipher.DECRYPT_MODE, secretKeySpec, spec);
      return cipher.doFinal(ciphertext, inputOffset, inputLen, plaintext, outputOffset);
    } catch (BadPaddingException | InvalidAlgorithmParameterException | IllegalBlockSizeException
        | InvalidKeyException | NoSuchAlgorithmException | NoSuchPaddingException
        | NoSuchProviderException | ShortBufferException e) {
      throw new RuntimeException("Failed to decrypt the plaintext with given key ", e);
    }
  }

  /**
   * Decrypts the input ByteBuf and return the ciphertext in another DataBuffer. This takes the
   * ownership of the input DataBuffer.
   *
   * @param cryptoKey the crypto key which contains the decryption key, iv, authTag and etc
   * @param input the input ciphertext in a DataBuffer
   * @return the decrypted content in a byte array
   */
  public static byte[] decrypt(CryptoKey cryptoKey, DataBuffer input) {
    final int chunkFooterSize =
        (int) Configuration.getBytes(PropertyKey.USER_ENCRYPTION_CHUNK_FOOTER_SIZE_BYTES);
    final int physicalChunkSize = (int)
        (Configuration.getBytes(PropertyKey.USER_ENCRYPTION_CHUNK_HEADER_SIZE_BYTES)
            + Configuration.getBytes(PropertyKey.USER_ENCRYPTION_CHUNK_SIZE_BYTES)
            + chunkFooterSize);

    // TODO(chaomin): maybe other possibilities?
    Preconditions.checkState(input instanceof DataByteBuffer || input instanceof DataNettyBufferV2,
        "input DataBuffer must be either DataByteBuffer or DataNettyBufferV2.");
    boolean isDataBufferV2 = input instanceof DataNettyBufferV2;
    ByteBuffer inputBufV1 = isDataBufferV2 ? null : input.getReadOnlyByteBuffer();
    // Physical chunk size is either a full physical chunk, or the last chunk to EOF.
    int physicalTotalLen = isDataBufferV2 ? input.readableBytes() : inputBufV1.capacity();
    int logicalTotalLen = (int) LayoutUtils.toLogicalLength(SPEC, 0, physicalTotalLen);
    byte[] plaintext = new byte[logicalTotalLen];
    byte[] cipherChunk = new byte[physicalChunkSize];
    try {
      byte[] key = cryptoKey.getKey();
      MessageDigest sha = MessageDigest.getInstance(SHA1);
      key = sha.digest(key);
      byte[] encryptKey = Arrays.copyOf(key, AES_KEY_LENGTH); // use only first 16 bytes
      SecretKeySpec secretKeySpec = new SecretKeySpec(encryptKey, AES);
      GCMParameterSpec spec = new GCMParameterSpec(GCM_TAG_LENGTH * 8, cryptoKey.getIv());

      int logicalPos = 0;
      int physicalPos = 0;
      while (physicalPos < physicalTotalLen) {
        int physicalLeft = physicalTotalLen - physicalPos;
        int physicalChunkLen = Math.min(physicalChunkSize, physicalLeft);
        if (isDataBufferV2) {
          input.readBytes(cipherChunk, 0 /* dest chunk */, physicalChunkLen /* len */);
        } else {
          inputBufV1.get(cipherChunk, 0 /* dest index */, physicalChunkLen /* len */);
        }
        Cipher cipher = Cipher.getInstance(cryptoKey.getCipher(), "SunJCE");
        cipher.init(Cipher.DECRYPT_MODE, secretKeySpec, spec);
        // Decryption always stops at the chunk boundary, with either a full chunk or the last
        // chunk till the end of the block.
        int logicalChunkLen =
            cipher.doFinal(cipherChunk, 0, physicalChunkLen, plaintext, logicalPos);
        Preconditions.checkState(logicalChunkLen + chunkFooterSize == physicalChunkLen);
        logicalPos += logicalChunkLen;
        physicalPos += physicalChunkLen;
      }
      Preconditions.checkState(logicalPos == logicalTotalLen);
      return plaintext;
    } catch (BadPaddingException | InvalidAlgorithmParameterException | IllegalBlockSizeException
        | InvalidKeyException | NoSuchAlgorithmException | NoSuchPaddingException
        | NoSuchProviderException | ShortBufferException e) {
      throw new RuntimeException("Failed to decrypt the plaintext with given key ", e);
    } finally {
      input.release();
    }
  }

  private CryptoUtils() {} // prevent instantiation
}
