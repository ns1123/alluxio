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

import alluxio.Configuration;
import alluxio.PropertyKey;

import com.google.common.base.Preconditions;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The util class for block and chunk layout translation.
 */
@ThreadSafe
public final class LayoutUtils {
  /**
   * Translates a logical offset to the physical chunk start offset.
   *
   * @param spec the layout spec
   * @param logicalOffset the logical offset
   * @return the translated chunk physical start
   */
  public static long getPhysicalChunkStart(LayoutSpec spec, long logicalOffset) {
    final long physicalChunkSize =
        spec.getChunkHeaderSize() + spec.getChunkSize() + spec.getChunkFooterSize();
    final long numChunksBeforeOffset = logicalOffset / spec.getChunkSize();
    return spec.getBlockHeaderSize() + numChunksBeforeOffset * physicalChunkSize;
  }

  /**
   * Translates a logical offset to a physical offset starting from the physical chunk start.
   *
   * @param spec the layout spec
   * @param logicalOffset the logical offset
   * @return the translated physical offset from the chunk physical start
   */
  public static long getPhysicalOffsetFromChunkStart(LayoutSpec spec, long logicalOffset) {
    return spec.getChunkHeaderSize() + logicalOffset % spec.getChunkSize();
  }

  /**
   * Translates a logical offset to a physical offset.
   *
   * @param spec the layout spec
   * @param logicalOffset the logical offset
   * @return the translated physical offset
   */
  public static long toPhysicalOffset(LayoutSpec spec, long logicalOffset) {
    return getPhysicalChunkStart(spec, logicalOffset)
        + getPhysicalOffsetFromChunkStart(spec, logicalOffset);
  }

  /**
   * Translates a physical offset to a logical offset.
   *
   * @param spec the layout spec
   * @param physicalOffset the physical offset
   * @return the translated logical offset
   */
  public static long toLogicalOffset(LayoutSpec spec, long physicalOffset) {
    final long blockHeaderSize = spec.getBlockHeaderSize();
    final long chunkSize = spec.getChunkSize();
    final long physicalChunkSize = spec.getChunkHeaderSize() + chunkSize + spec.getChunkFooterSize();
    final long numChunksBeforeOffset = (physicalOffset - blockHeaderSize) / physicalChunkSize;
    Preconditions.checkState(physicalOffset >= blockHeaderSize + spec.getChunkHeaderSize());
    final long logicalOffsetInChunk =
        (physicalOffset - blockHeaderSize) % physicalChunkSize - spec.getChunkHeaderSize();
    Preconditions.checkState(logicalOffsetInChunk >= 0 && logicalOffsetInChunk < chunkSize);
    return numChunksBeforeOffset * chunkSize + logicalOffsetInChunk;
  }

  /**
   * Translates a logical length to a physical length, given the logical offset.
   * It is assumed that the physical length reaches the end of a chunk, with inclusive chunk footer.
   *
   * @param spec the layout spec
   * @param logicalOffset the logical offset
   * @param logicalLength the logical length
   * @return the physical length
   */
  public static long toPhysicalLength(LayoutSpec spec, long logicalOffset, long logicalLength) {
    final long bytesLeftInChunk = spec.getChunkSize() - logicalOffset % spec.getChunkSize();
    final long chunkSize = spec.getChunkSize();
    if (logicalLength <= bytesLeftInChunk) {
      return logicalLength + spec.getChunkFooterSize();
    }
    final long physicalChunkSize =
        spec.getChunkHeaderSize() + chunkSize + spec.getChunkFooterSize();
    long secondPart = logicalLength - bytesLeftInChunk;
    return bytesLeftInChunk + spec.getChunkFooterSize() + secondPart / chunkSize * physicalChunkSize
        + spec.getChunkHeaderSize() + secondPart % chunkSize + spec.getChunkFooterSize();
  }

  /**
   * Translates a physical length to a logical length, given the physical offset.
   * It is assumed that the physical length reaches the end of a chunk, with inclusive chunk footer.
   *
   * @param spec the layout spec
   * @param physicalOffset the physical offset
   * @param physicalLength the physical length
   * @return the logical length
   */
  public static long toLogicalLength(LayoutSpec spec, long physicalOffset, long physicalLength) {
    final long chunkHeaderSize = spec.getChunkHeaderSize();
    final long chunkSize = spec.getChunkSize();
    final long chunkFooterSize = spec.getChunkFooterSize();
    final long physicalChunkSize = chunkHeaderSize + chunkSize + chunkFooterSize;
    // Adjust the physical offset 0 to start at the logical offset 0.
    final long adjustedPhysicalOffset = physicalOffset == 0
        ? spec.getBlockHeaderSize() + chunkHeaderSize : physicalOffset;
    Preconditions.checkState(adjustedPhysicalOffset >= spec.getBlockHeaderSize() + chunkHeaderSize);
    Preconditions.checkState(physicalLength >= chunkFooterSize);
    final long logicalOffsetInChunk =
        (adjustedPhysicalOffset - spec.getBlockHeaderSize()) % physicalChunkSize - chunkHeaderSize;
    Preconditions.checkState(logicalOffsetInChunk >= 0 && logicalOffsetInChunk < chunkSize);
    final long bytesLeftInChunk = chunkSize - logicalOffsetInChunk;
    if (physicalLength <= bytesLeftInChunk + chunkFooterSize) {
      return physicalLength - chunkFooterSize;
    }
    final long secondPart = physicalLength - bytesLeftInChunk - chunkFooterSize;
    final long physicalLengthInLastChunk = secondPart % physicalChunkSize;
    Preconditions.checkState(physicalLengthInLastChunk == 0
        || (physicalLengthInLastChunk > chunkHeaderSize
        && physicalLengthInLastChunk < chunkHeaderSize + chunkSize));
    return bytesLeftInChunk + secondPart / physicalChunkSize * chunkSize
        + (physicalLengthInLastChunk - chunkHeaderSize - chunkFooterSize);
  }

  /**
   * @return the {@link LayoutUtils} from configuration
   */
  public static LayoutSpec createLayoutSpecFromConfiguration() {
    long defaultBlockSize = Configuration.getBytes(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT);
    long chunkSize = Configuration.getBytes(PropertyKey.USER_ENCRYPTION_CHUNK_SIZE_BYTES);
    return new LayoutSpec(
        Configuration.getBytes(PropertyKey.USER_BLOCK_HEADER_SIZE_BYTES),
        Configuration.getBytes(PropertyKey.USER_BLOCK_FOOTER_SIZE_BYTES),
        defaultBlockSize < chunkSize ? 0 : defaultBlockSize, /* for tests */
        Configuration.getBytes(PropertyKey.USER_ENCRYPTION_CHUNK_HEADER_SIZE_BYTES),
        chunkSize,
        Configuration.getBytes(PropertyKey.USER_ENCRYPTION_CHUNK_FOOTER_SIZE_BYTES));
  }

  private LayoutUtils() {} // prevent instantiation
}
