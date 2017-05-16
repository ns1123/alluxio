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

package alluxio.client.block.stream;

import alluxio.Constants;
import alluxio.client.LayoutUtils;
import alluxio.client.security.CryptoKey;
import alluxio.client.security.CryptoUtils;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.network.protocol.databuffer.DataNettyBufferV2;
import alluxio.proto.security.EncryptionProto;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;

/**
 * The wrapper on {@link PacketReader} to read packets with decryption.
 */
public class CryptoPacketReader implements PacketReader {
  private static final String CIPHER_NAME = "AES/GCM/NoPadding";

  private PacketReader mPacketReader;
  private long mLogicalPos;
  private long mLogicalLen;
  /** The logical offset within the chunk of the initial logical pos. */
  private int mInitialOffsetFromChunkStart;
  private EncryptionProto.Meta mMeta;
  private boolean mCryptoMode;
  // TODO(chaomin): add a bit to indicate whether this is the end of file or not

  /**
   * Creates a {@link CryptoPacketReader} with a non-crypto {@link PacketReader}.
   *
   * @param packetReader the non-crypto packet reader
   * @param offset the stream offset
   * @param len the length of the stream
   * @param meta the encryption metadata
   */
  public CryptoPacketReader(
      PacketReader packetReader, long offset, long len, EncryptionProto.Meta meta) {
    mPacketReader = packetReader;
    mLogicalPos = offset;
    mLogicalLen = len;
    mMeta = meta;
    mCryptoMode = true;
    mInitialOffsetFromChunkStart =
        (int) LayoutUtils.getPhysicalOffsetFromChunkStart(mMeta, mLogicalPos);
  }

  /**
   * Sets the crypto mode to on or off.
   *
   * @param cryptoMode the crypto mode
   */
  public void setCryptoMode(boolean cryptoMode) {
    mCryptoMode = cryptoMode;
  }

  /**
   * Reads and decrypts a packet. The caller needs to release the packet.
   *
   * @return the data buffer in plaintext or null if EOF is reached
   */
  @Override
  public DataBuffer readPacket() throws IOException {
    if (!mCryptoMode) {
      return mPacketReader.readPacket();
    }
    DataBuffer cipherBuffer = mPacketReader.readPacket();
    if (cipherBuffer == null || cipherBuffer.readableBytes() == 0) {
      return null;
    }
    CryptoKey decryptKey = new CryptoKey(
        CIPHER_NAME, Constants.ENCRYPTION_KEY_FOR_TESTING.getBytes(),
        Constants.ENCRYPTION_IV_FOR_TESTING.getBytes(), true);
    // Note: cipherBuffer is released by decryptChunks.
    // TODO(chaomin): need to distinguish the first packet of a block when block header is not empty
    byte[] plaintext = CryptoUtils.decryptChunks(mMeta, decryptKey, cipherBuffer);
    int logicalLen = (int) Math.min(plaintext.length - mInitialOffsetFromChunkStart, mLogicalLen);
    // TODO(chaomin): avoid heavy use of Unpooled buffer.
    ByteBuf byteBuf = Unpooled.wrappedBuffer(plaintext, mInitialOffsetFromChunkStart, logicalLen);
    mInitialOffsetFromChunkStart = 0;
    mLogicalPos += logicalLen;
    mLogicalLen -= logicalLen;
    return new DataNettyBufferV2(byteBuf);
  }

  @Override
  public long pos() {
    return mLogicalPos;
  }

  @Override
  public void close() throws IOException {
    mPacketReader.close();
  }

  /**
   * The factory to create {@link CryptoPacketReader}s.
   */
  public static class Factory implements PacketReader.Factory {
    private PacketReader.Factory mFactory;
    private EncryptionProto.Meta mMeta;

    /**
     * Creates a new factory with the non-crypto {@link PacketReader.Factory}.
     *
     * @param factory the non crypto factory
     * @param meta the encryption metadata
     */
    public Factory(PacketReader.Factory factory, EncryptionProto.Meta meta) {
      mFactory = factory;
      mMeta = meta;
    }

    @Override
    public CryptoPacketReader create(long offset, long len) throws IOException {
      // Always read a total length that is multiple of the physical chunk size, otherwise it
      // is not possible to decrypt.
      long physicalReadLength =
          (LayoutUtils.getPhysicalOffsetFromChunkStart(mMeta, offset)
              + len + mMeta.getChunkSize() - 1) / mMeta.getChunkSize()
              * (mMeta.getChunkHeaderSize() + mMeta.getChunkSize() + mMeta.getChunkFooterSize());
      return new CryptoPacketReader(
          mFactory.create(LayoutUtils.getPhysicalChunkStart(mMeta, offset), physicalReadLength),
          offset, len, mMeta);
    }

    @Override
    public boolean isShortCircuit() {
      return mFactory.isShortCircuit();
    }
  }
}
