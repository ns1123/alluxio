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

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.client.LayoutSpec;
import alluxio.client.LayoutUtils;
import alluxio.client.security.CryptoKey;
import alluxio.client.security.CryptoUtils;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.network.protocol.databuffer.DataNettyBufferV2;

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
  private LayoutSpec mSpec = LayoutSpec.Factory.createFromConfiguration();

  /**
   * Creates a {@link CryptoPacketReader} with a non-crypto {@link PacketReader}.
   *
   * @param packetReader the non-crypto packet reader
   * @param offset the stream offset
   * @param len the length of the stream
   */
  public CryptoPacketReader(PacketReader packetReader, long offset, long len) {
    mPacketReader = packetReader;
    mLogicalPos = offset;
    mLogicalLen = len;
    mInitialOffsetFromChunkStart =
        (int) LayoutUtils.getPhysicalOffsetFromChunkStart(mSpec, mLogicalPos);
  }

  /**
   * Reads and decrypts a packet. The caller needs to release the packet.
   *
   * @return the data buffer in plaintext or null if EOF is reached
   * @throws IOException if it fails to read a packet
   */
  @Override
  public DataBuffer readPacket() throws IOException {
    CryptoKey decryptKey = new CryptoKey(
        CIPHER_NAME, Constants.ENCRYPTION_KEY_FOR_TESTING.getBytes(),
        Constants.ENCRYPTION_IV_FOR_TESTING.getBytes(), true);
    DataBuffer cipherBuffer = mPacketReader.readPacket();
    // TODO(chaomin): need to distinguish the first packet of a block when block header is not empty
    byte[] plaintext = CryptoUtils.decryptChunks(mSpec, decryptKey, cipherBuffer);
    int logicalLen = (int) Math.min(plaintext.length - mInitialOffsetFromChunkStart,
        mLogicalLen - mLogicalPos);
    ByteBuf byteBuf = Unpooled.wrappedBuffer(plaintext, mInitialOffsetFromChunkStart, logicalLen);
    mInitialOffsetFromChunkStart = 0;
    mLogicalPos += logicalLen;
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
    private LayoutSpec mSpec = LayoutSpec.Factory.createFromConfiguration();
    private final long mChunkHeaderSize =
        Configuration.getBytes(PropertyKey.USER_ENCRYPTION_CHUNK_HEADER_SIZE_BYTES);
    private final long mChunkSize =
        Configuration.getBytes(PropertyKey.USER_ENCRYPTION_CHUNK_SIZE_BYTES);
    private final long mChunkFooterSize =
        Configuration.getBytes(PropertyKey.USER_ENCRYPTION_CHUNK_FOOTER_SIZE_BYTES);
    private final long mPhysicalChunkSize =
        mChunkHeaderSize + mChunkSize + mChunkFooterSize;

    /**
     * Creates a new factory with the non-crypto {@link PacketReader.Factory}.
     *
     * @param factory the non crypto factory
     */
    public Factory(PacketReader.Factory factory) {
      mFactory = factory;
    }

    @Override
    public CryptoPacketReader create(long offset, long len) throws IOException {
      // Always read a total length that is multiple of the physical chunk size, otherwise it
      // is not possible to decrypt.
      long physicalReadLength =
          (LayoutUtils.getPhysicalOffsetFromChunkStart(mSpec, offset) + len + mChunkSize - 1)
              / mChunkSize * mPhysicalChunkSize;
      return new CryptoPacketReader(
          mFactory.create(LayoutUtils.getPhysicalChunkStart(mSpec, offset), physicalReadLength),
          offset, len);
    }

    @Override
    public boolean isShortCircuit() {
      return mFactory.isShortCircuit();
    }
  }
}
