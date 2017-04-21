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
  public static int getPhysicalChunkStart(LayoutSpec spec, int logicalOffset) {
    final int physicalChunkSize =
        spec.getChunkHeaderSize() + spec.getChunkSize() + spec.getChunkFooterSize();
    final int numChunksBeforeOffset = logicalOffset / spec.getChunkSize();
    return spec.getBlockHeaderSize() + numChunksBeforeOffset * physicalChunkSize;
  }

  /**
   * Translates a logical offset to a physical offset starting from the physical chunk start.
   *
   * @param spec the layout spec
   * @param logicalOffset the logical offset
   * @return the translated physical offset from the chunk physical start
   */
  public static int getPhysicalOffsetFromChunkStart(LayoutSpec spec, int logicalOffset) {
    return spec.getChunkHeaderSize() + logicalOffset % spec.getChunkSize();
  }

  /**
   * Translates a logical offset to a physical offset.
   *
   * @param spec the layout spec
   * @param logicalOffset the logical offset
   * @return the translated physical offset
   */
  public static int logicalOffsetToPhysicalOffset(LayoutSpec spec, int logicalOffset) {
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
  public static int physicalOffsetToLogicalOffset(LayoutSpec spec, int physicalOffset) {
    final int blockHeaderSize = spec.getBlockHeaderSize();
    final int chunkSize = spec.getChunkSize();
    final int physicalChunkSize = spec.getChunkHeaderSize() + chunkSize + spec.getChunkFooterSize();
    final int numChunksBeforeOffset = (physicalOffset - blockHeaderSize) / physicalChunkSize;
    Preconditions.checkState(physicalOffset >= blockHeaderSize + spec.getChunkHeaderSize());
    final int logicalOffsetInChunk =
        (physicalOffset - blockHeaderSize) % physicalChunkSize - spec.getChunkHeaderSize();
    Preconditions.checkState(logicalOffsetInChunk >= 0 && logicalOffsetInChunk < chunkSize);
    return numChunksBeforeOffset * chunkSize + logicalOffsetInChunk;
  }

  /**
   * Translates a logical length to a physical length, given the logical offset.
   *
   * @param spec the layout spec
   * @param logicalOffset the logical offset
   * @param logicalLength the logical length
   * @return the physical length
   */
  public static int logicalLengthToPhysicalLength(
      LayoutSpec spec, int logicalOffset, int logicalLength) {
    final int bytesLeftInChunk = spec.getChunkSize() - logicalOffset % spec.getChunkSize();
    final int chunkSize = spec.getChunkSize();
    if (logicalLength < bytesLeftInChunk) {
      return logicalLength;
    } else if (logicalLength == bytesLeftInChunk) {
      return logicalLength + spec.getChunkFooterSize();
    }
    final int physicalChunkSize = spec.getChunkHeaderSize() + chunkSize + spec.getChunkFooterSize();
    int secondPart = logicalLength - bytesLeftInChunk;
    return bytesLeftInChunk + spec.getChunkFooterSize() + secondPart / chunkSize * physicalChunkSize
        + spec.getChunkHeaderSize() + secondPart % chunkSize;
  }

  /**
   * Translates a physical length to a logical length, given the physical offset.
   *
   * @param spec the layout spec
   * @param physicalOffset the physical offset
   * @param physicalLength the physical length
   * @return the logical length
   */
  public static int physicalLengthToLogicalLength(
      LayoutSpec spec, int physicalOffset, int physicalLength) {
    final int chunkHeaderSize = spec.getChunkHeaderSize();
    final int chunkSize = spec.getChunkSize();
    final int physicalChunkSize = chunkHeaderSize + chunkSize + spec.getChunkFooterSize();
    Preconditions.checkState(physicalOffset >= spec.getBlockHeaderSize() + chunkHeaderSize);
    final int logicalOffsetInChunk =
        (physicalOffset - spec.getBlockHeaderSize()) % physicalChunkSize - chunkHeaderSize;
    Preconditions.checkState(logicalOffsetInChunk >= 0 && logicalOffsetInChunk < chunkSize);
    final int bytesLeftInChunk = chunkSize - logicalOffsetInChunk;
    if (physicalLength <= bytesLeftInChunk) {
      return physicalLength;
    } else if (physicalLength <= bytesLeftInChunk + spec.getChunkFooterSize()) {
      Preconditions.checkState(physicalLength == bytesLeftInChunk + spec.getChunkFooterSize());
      return bytesLeftInChunk;
    }
    final int secondPart = physicalLength - bytesLeftInChunk - spec.getChunkFooterSize();
    final int physicalLengthInLastChunk = secondPart % physicalChunkSize;
    Preconditions.checkState(physicalLengthInLastChunk == 0
        || (physicalLengthInLastChunk > chunkHeaderSize
        && physicalLengthInLastChunk < chunkHeaderSize + chunkSize));
    return bytesLeftInChunk + secondPart / physicalChunkSize * chunkSize
        + (physicalLengthInLastChunk - chunkHeaderSize);
  }

  private LayoutUtils() {} // prevent instantiation
}
