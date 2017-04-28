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
import alluxio.client.block.BlockWorkerClient;
import alluxio.client.security.CryptoKey;
import alluxio.client.security.CryptoUtils;
import alluxio.worker.block.io.LocalFileBlockWriter;

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A local packet writer with encryption.
 * TODO(chaomin): this is just a prototype, need to dedup the logic with
 * {@link LocalFilePacketWriter}.
 */
@NotThreadSafe
public final class CryptoLocalFilePacketWriter implements PacketWriter {
  private static final long PACKET_SIZE =
      Configuration.getBytes(PropertyKey.USER_LOCAL_WRITER_PACKET_SIZE_BYTES);
  private static final long FILE_BUFFER_BYTES =
      Configuration.getBytes(PropertyKey.USER_FILE_BUFFER_BYTES);
  private static final String CIPHER_NAME = "AES/GCM/NoPadding";
  private static final long CHUNK_HEADER_SIZE =
      Configuration.getBytes(PropertyKey.USER_ENCRYPTION_CHUNK_HEADER_SIZE_BYTES);
  private static final long CHUNK_SIZE =
      Configuration.getBytes(PropertyKey.USER_ENCRYPTION_CHUNK_SIZE_BYTES);
  private static final long CHUNK_FOOTER_SIZE =
      Configuration.getBytes(PropertyKey.USER_ENCRYPTION_CHUNK_FOOTER_SIZE_BYTES);
  private static final long CHUNKS_PER_PACKET = PACKET_SIZE / CHUNK_SIZE;

  /** The logical position to write the next byte at. */
  private long mLogicalPos = 0;
  /** The number of physical bytes reserved on the block worker to hold the block. */
  private long mPosReserved = 0;
  private final long mBlockId;
  private final LocalFileBlockWriter mWriter;
  private final BlockWorkerClient mBlockWorkerClient;
  private boolean mClosed = false;
  private final LayoutSpec mLayoutSpec = LayoutUtils.createLayoutSpecFromConfiguration();

  /**
   * Creates an instance of {@link CryptoLocalFilePacketWriter}. This requires the block to be locked
   * beforehand.
   *
   * @param blockWorkerClient the block worker client, not owned by this class
   * @param blockId the block ID
   * @param blockSize the block size
   * @param tier the target tier
   * @throws IOException if it fails to create the packet writer
   * @return the {@link CryptoLocalFilePacketWriter} created
   */
  public static CryptoLocalFilePacketWriter create(
      BlockWorkerClient blockWorkerClient, long blockId, long blockSize, int tier)
      throws IOException {
    Preconditions.checkState(PACKET_SIZE % CHUNK_SIZE == 0);
    Preconditions.checkState(CHUNKS_PER_PACKET >= 1);
    return new CryptoLocalFilePacketWriter(blockWorkerClient, blockId, blockSize, tier);
  }

  @Override
  public long pos() {
    return mLogicalPos;
  }

  @Override
  public int packetSize() {
    return (int) PACKET_SIZE;
  }

  @Override
  public void writePacket(final ByteBuf buf) throws IOException {
    try {
      Preconditions.checkState(!mClosed, "PacketWriter is closed while writing packets.");
      int sz = buf.readableBytes();
      int physicalTotalLen = (int) LayoutUtils.toPhysicalLength(mLayoutSpec, 0, sz);
      byte[] ciphertext = new byte[physicalTotalLen];
      byte[] plainChunk = new byte[(int) CHUNK_SIZE];
      int logicalPos = 0;
      int physicalPos = 0;
      while (logicalPos < sz) {
        // Encrypt chunk by chunk. Write and flush small amount of data is not yet supported.
        // It is required to write all data at once, or write and flush at chunk boundaries.
        int logicalChunkLen = Math.min(sz - logicalPos, (int) CHUNK_SIZE);
        buf.getBytes(logicalPos, plainChunk, 0 /* dest index */, logicalChunkLen /* len */);
        CryptoKey encryptKey = new CryptoKey(
            CIPHER_NAME, Constants.ENCRYPTION_KEY_FOR_TESTING.getBytes(),
            Constants.ENCRYPTION_IV_FOR_TESTING.getBytes(), true);
        int physicalLen = CryptoUtils.encrypt(
            encryptKey, plainChunk, 0, logicalChunkLen, ciphertext, physicalPos);
        Preconditions.checkState(physicalLen == logicalChunkLen + CHUNK_FOOTER_SIZE);
        logicalPos += logicalChunkLen;
        physicalPos += physicalLen;
      }
      Preconditions.checkState(physicalPos == physicalTotalLen);
      ByteBuf encryptedBuf = Unpooled.wrappedBuffer(ciphertext);
      try {
        ensureReserved(physicalTotalLen);
        Preconditions.checkState(
            encryptedBuf.readBytes(mWriter.getChannel(), physicalTotalLen) == physicalTotalLen);
      } finally {
        encryptedBuf.release();
      }
      mLogicalPos += logicalPos;
    } finally {
      buf.release();
    }
  }

  @Override
  public void cancel() throws IOException {
    close();
  }

  @Override
  public void flush() {}

  @Override
  public void close() throws IOException {
    if (mClosed) {
      return;
    }
    try {
      mWriter.close();
    } finally {
      mClosed = true;
    }
  }

  /**
   * Creates an instance of {@link CryptoLocalFilePacketWriter}.
   *
   * @param blockWorkerClient the block worker client, not owned by this class
   * @param blockId the block ID
   * @param tier the target tier
   * @throws IOException if it fails to create the packet writer
   */
  private CryptoLocalFilePacketWriter(
      BlockWorkerClient blockWorkerClient, long blockId, long blockSize, int tier)
      throws IOException {
    String blockPath = blockWorkerClient.requestBlockLocation(blockId, FILE_BUFFER_BYTES, tier);
    mWriter = new LocalFileBlockWriter(blockPath);
    mPosReserved += FILE_BUFFER_BYTES;
    mBlockId = blockId;
    mBlockWorkerClient = blockWorkerClient;
  }

  /**
   * Reserves enough space in the block worker.
   *
   * @param pos the pos of the file/block to reserve to
   * @throws IOException if it fails to reserve the space
   */
  private void ensureReserved(long pos) throws IOException {
    if (pos <= mPosReserved) {
      return;
    }
    long toReserve = Math.max(pos - mPosReserved, FILE_BUFFER_BYTES);
    mBlockWorkerClient.requestSpace(mBlockId, toReserve);
    mPosReserved += toReserve;
  }
}

