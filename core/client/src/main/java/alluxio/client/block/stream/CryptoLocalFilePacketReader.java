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
import alluxio.network.protocol.databuffer.DataByteBuffer;
import alluxio.worker.block.io.LocalFileBlockReader;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.nio.ByteBuffer;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A local packet reader with encryption.
 * TODO(chaomin): this is just a prototype, need to dedup the logic with
 * {@link LocalFilePacketReader}.
 */
@NotThreadSafe
public final class CryptoLocalFilePacketReader implements PacketReader {
  private static final long LOCAL_READ_PACKET_SIZE =
      Configuration.getBytes(PropertyKey.USER_LOCAL_READER_PACKET_SIZE_BYTES);
  private static final String CIPHER_NAME = "AES/GCM/NoPadding";
  private static final long BLOCK_HEADER_SIZE =
      Configuration.getBytes(PropertyKey.USER_BLOCK_HEADER_SIZE_BYTES);
  private static final long BLOCK_FOOTER_SIZE =
      Configuration.getBytes(PropertyKey.USER_BLOCK_FOOTER_SIZE_BYTES);
  private static final long CHUNK_HEADER_SIZE =
      Configuration.getBytes(PropertyKey.USER_ENCRYPTION_CHUNK_HEADER_SIZE_BYTES);
  private static final long CHUNK_SIZE =
      Configuration.getBytes(PropertyKey.USER_ENCRYPTION_CHUNK_SIZE_BYTES);
  private static final long CHUNK_FOOTER_SIZE =
      Configuration.getBytes(PropertyKey.USER_ENCRYPTION_CHUNK_FOOTER_SIZE_BYTES);
  private static final long CHUNKS_PER_PACKET = (LOCAL_READ_PACKET_SIZE / CHUNK_SIZE);

  /** The file reader to read a local block. */
  private final LocalFileBlockReader mReader;

  private LayoutSpec mLayoutSpec;
  /** The logical position to read the next byte at. */
  private long mLogicalPos;
  /** The logical end position. */
  private final long mLogicalEnd;

  /**
   * Creates an instance of {@link CryptoLocalFilePacketReader}.
   *
   * @param reader the local file block reader
   * @param offset the offset
   * @param len the length to read
   */
  public CryptoLocalFilePacketReader(LocalFileBlockReader reader, long offset, long len) {
    mReader = reader;
    mLogicalPos = offset;
    mLogicalEnd = offset + len;
    mLayoutSpec = new LayoutSpec(BLOCK_HEADER_SIZE, BLOCK_FOOTER_SIZE,
        512 * Constants.MB, CHUNK_HEADER_SIZE, CHUNK_SIZE, CHUNK_FOOTER_SIZE);
  }

  @Override
  public DataBuffer readPacket() throws IOException {
    if (mLogicalPos >= mLogicalEnd) {
      return null;
    }
    byte[] plaintextBuffer =
        new byte[(int) (LOCAL_READ_PACKET_SIZE
            + CHUNKS_PER_PACKET * (CHUNK_HEADER_SIZE + CHUNK_FOOTER_SIZE))];
    int bufferPos = 0;
    while (mLogicalPos < mLogicalEnd) {
      // Decrypt chunk by chunk. Any random read requires reading the entire chunk for decryption.
      long physicalChunkStart = LayoutUtils.getPhysicalChunkStart(mLayoutSpec, mLogicalPos);
      long logicalOffsetInChunk = mLogicalPos % CHUNK_SIZE;
      long logicalReadLen =
          Math.min(CHUNK_SIZE - logicalOffsetInChunk, mLogicalEnd - mLogicalPos);
      long physicalReadLen = LayoutUtils.getPhysicalOffsetFromChunkStart(mLayoutSpec, mLogicalPos)
          + LayoutUtils.toPhysicalLength(mLayoutSpec, mLogicalPos, logicalReadLen);
      ByteBuffer buffer = mReader.read(physicalChunkStart, physicalReadLen);
      byte[] ciphertext = new byte[(int) physicalReadLen];
      buffer.get(ciphertext);
      CryptoKey decryptKey = new alluxio.client.security.CryptoKey(
          CIPHER_NAME, Constants.ENCRYPTION_KEY_FOR_TESTING.getBytes(),
          Constants.ENCRYPTION_IV_FOR_TESTING.getBytes(), true);
      byte[] plaintext = CryptoUtils.decrypt(ciphertext, decryptKey);
      Preconditions.checkState(
          plaintext.length == physicalReadLen - CHUNK_HEADER_SIZE - CHUNK_FOOTER_SIZE);
      // Get the logical data based on the logical offset and length.
      System.arraycopy(plaintext, (int) logicalOffsetInChunk, plaintextBuffer, bufferPos,
          (int) logicalReadLen);
      bufferPos += logicalReadLen;
      mLogicalPos += logicalReadLen;
    }
    ByteBuffer byteBuffer = ByteBuffer.wrap(plaintextBuffer, 0, bufferPos);
    DataBuffer dataBuffer = new DataByteBuffer(byteBuffer, byteBuffer.remaining());
    return dataBuffer;
  }

  @Override
  public long pos() {
    return mLogicalPos;
  }

  @Override
  public void close() throws IOException {
    mReader.close();
  }

  /**
   * Factory class to create {@link CryptoLocalFilePacketReader}s.
   */
  public static class Factory implements PacketReader.Factory {
    private final String mPath;

    /**
     * Creates an instance of {@link Factory}.
     *
     * @param path the file path
     */
    public Factory(String path) {
      mPath = path;
    }

    @Override
    public PacketReader create(long offset, long len) throws IOException {
      Preconditions.checkState(LOCAL_READ_PACKET_SIZE % CHUNK_SIZE == 0);
      Preconditions.checkState(CHUNKS_PER_PACKET >= 1);
      return new CryptoLocalFilePacketReader(new LocalFileBlockReader(mPath), offset, len);
    }

    @Override
    public boolean isShortCircuit() {
      return true;
    }
  }
}

