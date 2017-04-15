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

import alluxio.Constants;

import com.google.common.base.Preconditions;

/**
 * The util class to translate between logical and physical offset/length.
 */
public final class Translator {
  private int mBlockHeaderSize;
  private int mBlockFooterSize;
  private int mLogicalBlockSize;
  private int mChunkHeaderSize;
  private int mChunkSize;
  private int mChunkFooterSize;

  /**
   * Constructs a new {@link Translator} with default chunk size.
   * @param blockHeaderSize the block header size
   * @param blockFooterSize the block footer size
   * @param logicalBlockSize the logical block size
   */
  public Translator(int blockHeaderSize, int blockFooterSize, int logicalBlockSize) {
    this(blockHeaderSize, blockFooterSize, logicalBlockSize, Constants.DEFAULT_CHUNK_HEADER_SIZE,
        Constants.DEFAULT_CHUNK_SIZE, Constants.DEFAULT_CHUNK_FOOTER_SIZE);
  }

  /**
   * Constructs a new {@link Translator}.
   * @param blockHeaderSize the block header size
   * @param blockFooterSize the block footer size
   * @param logicalBlockSize the logical block size
   * @param chunkHeaderSize the chunk header size
   * @param chunkSize the chunk size
   * @param chunkFooterSize the chunk footer size
   */
  public Translator(int blockHeaderSize, int blockFooterSize, int logicalBlockSize,
                    int chunkHeaderSize, int chunkSize, int chunkFooterSize) {
    Preconditions.checkState(logicalBlockSize % chunkSize == 0);
    mBlockHeaderSize = blockHeaderSize;
    mBlockFooterSize = blockFooterSize;
    mLogicalBlockSize = logicalBlockSize;
    mChunkHeaderSize = chunkHeaderSize;
    mChunkSize = chunkSize;
    mChunkFooterSize = chunkFooterSize;
  }

  /**
   * @return the block header size
   */
  public int getBlockHeaderSize() {
    return mBlockHeaderSize;
  }

  /**
   * @return the block footer size
   */
  public int getBlockFooterSize() {
    return mBlockFooterSize;
  }

  /**
   * @return the logical block size
   */
  public int getLogicalBlockSize() {
    return mLogicalBlockSize;
  }

  /**
   * @return the chunk header size
   */
  public int getChunkHeaderSize() {
    return mChunkHeaderSize;
  }

  /**
   * @return the chunk size
   */
  public int getChunkSize() {
    return mChunkSize;
  }

  /**
   * @return the chunk footer size
   */
  public int getChunkFooterSize() {
    return mChunkFooterSize;
  }

  /**
   * @param blockHeaderSize the block header size to set
   * @return the updated object
   */
  public Translator setBlockHeaderSize(int blockHeaderSize) {
    mBlockHeaderSize = blockHeaderSize;
    return this;
  }

  /**
   * @param blockFooterSize the block footer size to set
   * @return the updated object
   */
  public Translator setBlockFooterSize(int blockFooterSize) {
    mBlockFooterSize = blockFooterSize;
    return this;
  }

  /**
   * @param logicalBlockSize the logical block size
   * @return the updated object
   */
  public Translator setLogicalBlockSize(int logicalBlockSize) {
    mLogicalBlockSize = logicalBlockSize;
    return this;
  }

  /**
   * @param chunkHeaderSize the chunk header size to set
   * @return the updated object
   */
  public Translator setChunkHeaderSize(int chunkHeaderSize) {
    mChunkHeaderSize = chunkHeaderSize;
    return this;
  }

  /**
   * @param chunkSize the chunk size to set
   * @return the updated object
   */
  public Translator setChunkSize(int chunkSize) {
    mChunkSize = chunkSize;
    return this;
  }

  /**
   * @param chunkFooterSize the chunk footer size to set
   * @return the updated object
   */
  public Translator setChunkFooterSize(int chunkFooterSize) {
    mChunkFooterSize = chunkFooterSize;
    return this;
  }

  /**
   * Translates a logical offset to a physical offset.
   * @param logicalOffset the logical offset
   * @return the translated physical offset
   */
  public int logicalOffsetToPhysical(int logicalOffset) {
    final int physicalChunkSize = mChunkHeaderSize + mChunkSize + mChunkFooterSize;
    final int numChunksBeforeOffset = logicalOffset / mChunkSize;
    final int offsetInPhysicalChunk = mChunkHeaderSize + logicalOffset % mChunkSize;
    return mBlockHeaderSize + numChunksBeforeOffset * physicalChunkSize + offsetInPhysicalChunk;
  }

  /**
   * Translates a physical offset to a logical offset.
   * @param physicalOffset the physical offset
   * @return the translated logical offset
   */
  public int physicalOffsetToLogical(int physicalOffset) {
    final int physicalChunkSize = mChunkHeaderSize + mChunkSize + mChunkFooterSize;
    final int numChunksBeforeOffset = (physicalOffset - mBlockHeaderSize) / physicalChunkSize;
    Preconditions.checkState(physicalOffset >= mBlockHeaderSize + mChunkHeaderSize);
    final int logicalOffsetInChunk =
        (physicalOffset - mBlockHeaderSize) % physicalChunkSize - mChunkHeaderSize;
    Preconditions.checkState(logicalOffsetInChunk >= 0 && logicalOffsetInChunk < mChunkSize);
    return numChunksBeforeOffset * mChunkSize + logicalOffsetInChunk;
  }

  /**
   * Translates a logical length to a physical length, given the logical offset.
   * @param logicalOffset the logical offset
   * @param logicalLength the logical length
   * @return the physical length
   */
  public int logicalLengthToPhysical(int logicalOffset, int logicalLength) {
    final int bytesLeftInChunk = mChunkSize - logicalOffset % mChunkSize;
    if (logicalLength < bytesLeftInChunk) {
      return logicalLength;
    } else if (logicalLength == bytesLeftInChunk) {
      return logicalLength + mChunkFooterSize;
    }
    final int physicalChunkSize = mChunkHeaderSize + mChunkSize + mChunkFooterSize;
    int secondPart = logicalLength - bytesLeftInChunk;
    return bytesLeftInChunk + mChunkFooterSize + secondPart / mChunkSize * physicalChunkSize
        + mChunkHeaderSize + secondPart % mChunkSize;
  }

  /**
   * Translates a physical length to a logical length, given the physical offset.
   * @param physicalOffset the physical offset
   * @param physicalLength the physical length
   * @return the logical length
   */
  public int physicalLengthToLogical(int physicalOffset, int physicalLength) {
    final int physicalChunkSize = mChunkHeaderSize + mChunkSize + mChunkFooterSize;
    Preconditions.checkState(physicalOffset >= mBlockHeaderSize + mChunkHeaderSize);
    final int logicalOffsetInChunk =
        (physicalOffset - mBlockHeaderSize) % physicalChunkSize - mChunkHeaderSize;
    Preconditions.checkState(logicalOffsetInChunk >= 0 && logicalOffsetInChunk < mChunkSize);
    final int bytesLeftInChunk = mChunkSize - logicalOffsetInChunk;
    if (physicalLength <= bytesLeftInChunk) {
      return physicalLength;
    } else if (physicalLength <= bytesLeftInChunk + mChunkFooterSize) {
      Preconditions.checkState(physicalLength == bytesLeftInChunk + mChunkFooterSize);
      return bytesLeftInChunk;
    }
    final int secondPart = physicalLength - bytesLeftInChunk - mChunkFooterSize;
    final int physicalLengthInLastChunk = secondPart % physicalChunkSize;
    Preconditions.checkState(physicalLengthInLastChunk == 0
        || (physicalLengthInLastChunk > mChunkHeaderSize
            && physicalLengthInLastChunk < mChunkHeaderSize + mChunkSize));
    return bytesLeftInChunk + secondPart / physicalChunkSize * mChunkSize
        + (physicalLengthInLastChunk - mChunkHeaderSize);
  }
}
