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

package alluxio.client;

import alluxio.proto.journal.FileFooter;
import alluxio.proto.security.EncryptionProto;

import com.google.common.base.Preconditions;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The util class for block and chunk layout translation.
 */
@ThreadSafe
public final class LayoutUtils {
  private static final int FILE_METADATA_SIZE = initializeFileMetadataSize();

  /**
   * Translates a logical offset to the physical chunk start offset.
   *
   * @param meta the encryption metadata
   * @param logicalOffset the logical offset
   * @return the translated chunk physical start
   */
  public static long getPhysicalChunkStart(EncryptionProto.Meta meta, long logicalOffset) {
    final long physicalChunkSize =
        meta.getChunkHeaderSize() + meta.getChunkSize() + meta.getChunkFooterSize();
    final long numChunksBeforeOffset = logicalOffset / meta.getChunkSize();
    return meta.getBlockHeaderSize() + numChunksBeforeOffset * physicalChunkSize;
  }

  /**
   * Translates a logical offset to a physical offset starting from the physical chunk start.
   *
   * @param meta the encryption metadata
   * @param logicalOffset the logical offset
   * @return the translated physical offset from the chunk physical start
   */
  public static long getPhysicalOffsetFromChunkStart(
      EncryptionProto.Meta meta, long logicalOffset) {
    return meta.getChunkHeaderSize() + logicalOffset % meta.getChunkSize();
  }

  /**
   * Translates a logical offset to a physical offset.
   *
   * @param meta the encryption metadata
   * @param logicalOffset the logical offset
   * @return the translated physical offset
   */
  public static long toPhysicalOffset(EncryptionProto.Meta meta, long logicalOffset) {
    return getPhysicalChunkStart(meta, logicalOffset)
        + getPhysicalOffsetFromChunkStart(meta, logicalOffset);
  }

  /**
   * Translates a physical offset to a logical offset.
   * It is assumed that the physical offset reaches the end of a chunk, with inclusive chunk footer.
   *
   * @param meta the encryption metadata
   * @param physicalOffset the physical offset
   * @return the translated logical offset
   */
  public static long toLogicalOffset(EncryptionProto.Meta meta, long physicalOffset) {
    final long blockHeaderSize = meta.getBlockHeaderSize();
    if (physicalOffset == 0 || physicalOffset == blockHeaderSize) {
      return 0L;
    }
    final long chunkSize = meta.getChunkSize();
    final long physicalChunkSize =
        meta.getChunkHeaderSize() + meta.getChunkSize() + meta.getChunkFooterSize();
    final long numChunksBeforeOffset = (physicalOffset - blockHeaderSize) / physicalChunkSize;
    Preconditions.checkState(
        physicalOffset >= blockHeaderSize + meta.getChunkHeaderSize() + meta.getChunkFooterSize());
    final long secondPart = (physicalOffset - blockHeaderSize) % physicalChunkSize;
    final long logicalOffsetInChunk =
        secondPart == 0 ? 0 : secondPart - meta.getChunkHeaderSize() - meta.getChunkFooterSize();
    Preconditions.checkState(logicalOffsetInChunk >= 0 && logicalOffsetInChunk < chunkSize);
    return numChunksBeforeOffset * chunkSize + logicalOffsetInChunk;
  }

  /**
   * Translates a logical length to a physical length, given the logical offset.
   * It is assumed that the physical length reaches the end of a chunk, with inclusive chunk footer.
   *
   * @param meta the encryption metadata
   * @param logicalOffset the logical offset
   * @param logicalLength the logical length
   * @return the physical length
   */
  public static long toPhysicalLength(
      EncryptionProto.Meta meta, long logicalOffset, long logicalLength) {
    final long bytesLeftInChunk = meta.getChunkSize() - logicalOffset % meta.getChunkSize();
    final long chunkSize = meta.getChunkSize();
    if (logicalLength <= bytesLeftInChunk) {
      return logicalLength + meta.getChunkFooterSize();
    }
    final long physicalChunkSize =
        meta.getChunkHeaderSize() + meta.getChunkSize() + meta.getChunkFooterSize();
    long secondPart = logicalLength - bytesLeftInChunk;
    return bytesLeftInChunk + meta.getChunkFooterSize() + secondPart / chunkSize * physicalChunkSize
        + (secondPart % chunkSize == 0 ? 0
            : meta.getChunkHeaderSize() + secondPart % chunkSize + meta.getChunkFooterSize());
  }

  /**
   * Translates a physical length to a logical length, given the physical offset.
   * It is assumed that the physical length reaches the end of a chunk, with inclusive chunk footer.
   *
   * @param meta the encryption metadata
   * @param physicalOffset the physical offset
   * @param physicalLength the physical length
   * @return the logical length
   */
  public static long toLogicalLength(
      EncryptionProto.Meta meta, long physicalOffset, long physicalLength) {
    if (physicalLength == 0L) {
      return 0L;
    }
    final long chunkHeaderSize = meta.getChunkHeaderSize();
    final long chunkSize = meta.getChunkSize();
    final long chunkFooterSize = meta.getChunkFooterSize();
    final long physicalChunkSize = chunkHeaderSize + chunkSize + chunkFooterSize;
    // Adjust the physical offset 0 to start at the logical offset 0.
    final long adjustedPhysicalOffset = physicalOffset == 0
        ? meta.getBlockHeaderSize() + chunkHeaderSize : physicalOffset;
    Preconditions.checkState(adjustedPhysicalOffset >= meta.getBlockHeaderSize() + chunkHeaderSize);
    Preconditions.checkState(physicalLength >= chunkFooterSize);
    final long logicalOffsetInChunk =
        (adjustedPhysicalOffset - meta.getBlockHeaderSize()) % physicalChunkSize - chunkHeaderSize;
    Preconditions.checkState(logicalOffsetInChunk >= 0 && logicalOffsetInChunk < chunkSize);
    final long bytesLeftInChunk = chunkSize - logicalOffsetInChunk;
    if (physicalLength <= bytesLeftInChunk + chunkFooterSize) {
      return physicalLength - chunkFooterSize;
    }
    final long secondPart = physicalLength - bytesLeftInChunk - chunkFooterSize;
    final long physicalLengthInLastChunk = secondPart % physicalChunkSize;
    Preconditions.checkState(physicalLengthInLastChunk == 0
        || (physicalLengthInLastChunk > chunkHeaderSize + chunkFooterSize
        && physicalLengthInLastChunk < chunkHeaderSize + chunkSize + chunkFooterSize));
    return bytesLeftInChunk + secondPart / physicalChunkSize * chunkSize
        + (physicalLengthInLastChunk == 0 ? 0
            : (physicalLengthInLastChunk - chunkHeaderSize - chunkFooterSize));
  }

  /**
   * @return the file metadata size
   */
  public static int getFileMetadataSize() {
    return FILE_METADATA_SIZE;
  }

  /**
   * @return the file footer size
   */
  public static int getFooterSize() {
    return FILE_METADATA_SIZE + 8 /* fixed64 for file metadata size */  + 8 /* magic number size */;
  }

  private static int initializeFileMetadataSize() {
    FileFooter.FileMetadata fileMetadata = FileFooter.FileMetadata.newBuilder()
        .setBlockHeaderSize(1)
        .setBlockFooterSize(2)
        .setChunkHeaderSize(3)
        .setChunkSize(4)
        .setChunkFooterSize(5)
        .setPhysicalBlockSize(6)
        .setEncryptionId(7)
        .build();
    return fileMetadata.getSerializedSize();
  }

  private LayoutUtils() {} // prevent instantiation
}
